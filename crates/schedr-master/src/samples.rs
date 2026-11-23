//! A sample of each schedr process a minute (decision 464): the leading master writes its
//! masters' and workers' (one row per process, however many roles it runs: a standalone is one),
//! rolls each closed 15-minute window up into averages and peaks, and deletes minute rows past
//! a day and rollups past 30 days. A webserver in a process of its own writes its own.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_db::database::{Database, DbError};
use schedr_db::samples::{MINUTE, ROLLUP, Sample, Source};
use schedr_model::usage::{NodeUsage, WorkerParts};

#[cfg(test)]
mod tests;

/// How long after its end a window waits to be rolled up: a sample stored late (a webserver's
/// timer, a slow insert) is in it by then.
pub const ROLL_GRACE: SignedDuration = SignedDuration::from_mins(2);
/// How the samples are taken and kept (hidden timings, decision 464).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleSettings {
    /// How often a sample is taken (`MASTER__SAMPLE_INTERVAL`, a minute).
    pub interval: Duration,
    /// How long minute samples are kept (`MASTER__SAMPLES_KEPT`, a day).
    pub minutes_kept: Duration,
    /// How long rollups are kept (`MASTER__ROLLUPS_KEPT`, 30 days).
    pub rollups_kept: Duration,
    /// How long after its end a window waits to be rolled up (`MASTER__SAMPLE_ROLLUP_GRACE`,
    /// 2 minutes): a sample stored late is in it by then.
    pub rollup_grace: Duration,
    /// How often a master writes its row (`MASTER__NODE_HEARTBEAT`, 10 s): one heard from
    /// within three of them is sampled.
    pub node_heartbeat: Duration,
    /// The extension events kept per extension, the newest (`MASTER__EXTENSION_EVENTS_KEPT`,
    /// 200; DN19).
    pub events_kept: usize,
}

impl Default for SampleSettings {
    fn default() -> Self {
        Self {
            interval: Duration::from_mins(1),
            minutes_kept: Duration::from_hours(24),
            rollups_kept: Duration::from_hours(24 * 30),
            rollup_grace: ROLL_GRACE.unsigned_abs(),
            node_heartbeat: Duration::from_secs(10),
            events_kept: schedr_model::extension::KEPT_EVENTS,
        }
    }
}

/// A master heard from within this many of its heartbeats is sampled.
pub const MASTERS_HEARD_BEATS: u32 = 3;

/// A worker heard from within this long is sampled (its usage is written once a minute).
const WORKERS_HEARD: SignedDuration = SignedDuration::from_secs(120);

/// `at` down to a whole multiple of `step` seconds.
fn floor(at: Timestamp, step: u32) -> Timestamp {
    let step = i64::from(step);
    let seconds = at.as_second().div_euclid(step) * step;
    Timestamp::from_second(seconds).unwrap_or(at)
}

/// What a pass did, for the statement bound (decision 464): 3 a minute, 10 when a window
/// closes (its read and insert, the extensions' read (decision 513), then the four
/// deletes; 11 when the extensions have samples to insert), more at a new sampler's first pass,
/// which rolls every window not rolled yet.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pass {
    /// Samples written.
    pub written: usize,
    /// Rollups written.
    pub rolled: u64,
    /// The statements it made.
    pub statements: u32,
}

/// The sampler's memory between passes: each process's last CPU time with when it was measured
/// and the cores it gave, the last window rolled.
#[derive(Debug, Default)]
pub struct Sampler {
    cpu: HashMap<String, (Timestamp, f64, Option<f64>)>,
    rolled: Option<Timestamp>,
    /// A window was rolled since the deletes last succeeded (Grok's review of M5.1i: a failed
    /// delete waited for the next window).
    prune: bool,
}

impl Sampler {
    /// The cores `process` used between its last measure and this one (`cpu_seconds` measured
    /// `measured`), remembering it; none for its first, or when its CPU time went back (it
    /// restarted under the same ID). The same measure again (nothing stored since, Grok's
    /// review of M5.1i) gives the cores it gave before and keeps it as the baseline.
    fn cores(&mut self, process: &str, measured: Timestamp, cpu_seconds: f64) -> Option<f64> {
        if let Some((then, _, cores)) = self.cpu.get(process)
            && *then == measured
        {
            return *cores;
        }
        let before = self.cpu.get(process).copied();
        let cores = before.and_then(|(then, was, _)| {
            let elapsed = measured.duration_since(then).as_secs_f64();
            let used = cpu_seconds - was;
            (elapsed > 0.0 && used >= 0.0 && used.is_finite()).then(|| used / elapsed)
        });
        self.cpu
            .insert(process.to_owned(), (measured, cpu_seconds, cores));
        cores
    }

    /// One sample per process at minute `at` from `sources`: a process's roles together
    /// (with `own`'s roles added to the process `own` names: a standalone's webserver), its
    /// figures its worker's where it runs one (those have its parts).
    pub fn samples(
        &mut self,
        sources: Vec<Source>,
        at: Timestamp,
        own: Option<(&str, &BTreeMap<String, String>)>,
    ) -> Vec<Sample> {
        let mut by_process: BTreeMap<String, (BTreeMap<String, String>, Option<Source>)> =
            BTreeMap::new();
        for source in sources {
            let entry = by_process.entry(source.process_id.clone()).or_default();
            entry
                .0
                .insert(source.role.to_owned(), source.node_id.clone());
            let keep = match &entry.1 {
                None => true,
                Some(kept) => kept.parts.is_none() && source.parts.is_some(),
            };
            if keep {
                entry.1 = Some(source);
            }
        }
        if let Some((process, roles)) = own
            && let Some(entry) = by_process.get_mut(process)
        {
            entry
                .0
                .extend(roles.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        by_process
            .into_iter()
            .filter_map(|(process, (roles, source))| {
                let source = source?;
                Some(self.sample(
                    (process, roles),
                    &source.usage,
                    source.parts,
                    (at, source.at),
                ))
            })
            .collect()
    }

    /// `usage` of `process` (running `roles`), measured `measured`, as its minute sample at
    /// minute `at`.
    pub fn sample(
        &mut self,
        (process, roles): (String, BTreeMap<String, String>),
        usage: &NodeUsage,
        parts: Option<WorkerParts>,
        (at, measured): (Timestamp, Timestamp),
    ) -> Sample {
        let cpu = self.cores(&process, measured, usage.cpu_seconds);
        // Its whole memory: a worker's parts are processes of their own.
        let memory = usage
            .resident_bytes
            .saturating_add(parts.as_ref().map_or(0, |p| {
                p.fork_servers_bytes
                    .saturating_add(p.watcher_hosts_bytes)
                    .saturating_add(p.services_bytes)
                    .saturating_add(p.tasks_bytes)
            }));
        Sample {
            process_id: process,
            step_s: MINUTE,
            at,
            roles,
            resident_avg: usage.resident_bytes,
            resident_peak: usage.resident_bytes,
            memory_avg: memory,
            memory_peak: memory,
            minutes: 1,
            cpu_minutes: u32::from(cpu.is_some()),
            container_minutes: u32::from(usage.container_bytes.is_some()),
            covered: 1,
            cpu_seconds: Some(usage.cpu_seconds),
            cpu_avg: cpu,
            cpu_peak: cpu,
            container_avg: usage.container_bytes,
            container_peak: usage.container_bytes,
            container_limit: usage.container_limit_bytes,
            parts,
        }
    }

    /// The leader's pass at `now`: this minute's samples of the masters and workers, then,
    /// once a window has closed since the last one rolled, its rollup and the deletes.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails: the next pass goes on (a window not rolled is rolled
    /// then).
    pub async fn pass(
        &mut self,
        db: &Database,
        settings: &SampleSettings,
        now: Timestamp,
        own: Option<(&str, &BTreeMap<String, String>)>,
    ) -> Result<Pass, DbError> {
        let minute = floor(now, MINUTE);
        let sources = db
            .sample_sources(
                now.checked_sub(within(
                    settings.node_heartbeat.saturating_mul(MASTERS_HEARD_BEATS),
                ))
                .unwrap_or(now),
                now.checked_sub(WORKERS_HEARD).unwrap_or(now),
            )
            .await?;
        let mut pass = Pass {
            statements: 2,
            ..Pass::default()
        };
        let samples = self.samples(sources, minute, own);
        if !samples.is_empty() {
            db.insert_samples(&samples).await?;
            pass.statements += 1;
        }
        pass.written = samples.len();
        // Every window closed since the last one rolled (a new sampler: since the oldest
        // minute kept), each once (again after a restart: kept), before any minute is deleted
        // (Grok's review of M5.1i: a leader down across a boundary lost the windows it missed).
        // A window closes for rolling up a grace after its end, so that a webserver's sample
        // stored late is in it: a rollup isn't written again (Codex's review of M5.1j).
        let grace = within(settings.rollup_grace);
        let closed = floor(minute.checked_sub(grace).unwrap_or(minute), ROLLUP)
            .checked_sub(SignedDuration::from_secs(i64::from(ROLLUP)))
            .unwrap_or(minute);
        let window = SignedDuration::from_secs(i64::from(ROLLUP));
        let mut next = if let Some(rolled) = self.rolled {
            rolled.checked_add(window).unwrap_or(rolled)
        } else {
            pass.statements += 1;
            db.oldest_minute_sample()
                .await?
                .map_or(closed, |oldest| floor(oldest, ROLLUP))
        };
        // Nothing closed before the oldest minute: none to look for again.
        if next > closed && self.rolled.is_none() {
            self.rolled = Some(closed);
        }
        while next <= closed {
            let (rolled, statements) = roll_up(db, next).await?;
            pass.rolled += rolled;
            pass.statements += statements;
            self.rolled = Some(next);
            self.prune = true;
            match next.checked_add(window) {
                Ok(later) => next = later,
                Err(_) => break,
            }
        }
        if self.prune {
            let keep = |kept: Duration| {
                SignedDuration::try_from(kept)
                    .ok()
                    .and_then(|kept| now.checked_sub(kept).ok())
                    .unwrap_or(Timestamp::UNIX_EPOCH)
            };
            db.delete_old_samples(MINUTE, keep(settings.minutes_kept))
                .await?;
            db.delete_old_samples(ROLLUP, keep(settings.rollups_kept))
                .await?;
            db.delete_old_extension_samples(MINUTE, keep(settings.minutes_kept))
                .await?;
            db.delete_old_extension_samples(ROLLUP, keep(settings.rollups_kept))
                .await?;
            // An extension's events (DN19) as long as its rolled-up samples.
            db.delete_old_extension_events(keep(settings.rollups_kept))
                .await?;
            pass.statements += 5;
            self.prune = false;
        }
        Ok(pass)
    }
}

/// Rolls up the window starting `from`: its minute samples read, one row per process written
/// (averages rounded, peaks, its roles together); returns the rows written and the statements.
async fn roll_up(db: &Database, from: Timestamp) -> Result<(u64, u32), DbError> {
    let to = from
        .checked_add(SignedDuration::from_secs(i64::from(ROLLUP)))
        .unwrap_or(from);
    // The extensions' metrics' window too (decision 513), across workers.
    let extension_minutes = db.extension_samples(None, MINUTE, from, to).await?;
    let extensions = crate::figures::rolled_up(&extension_minutes, from);
    db.insert_extension_samples(&extensions).await?;
    let statements = if extensions.is_empty() { 1 } else { 2 };
    let minutes = db.samples(MINUTE, from, to).await?;
    let rolled = rolled_up(&minutes, from);
    if rolled.is_empty() {
        return Ok((0, 1 + statements));
    }
    db.insert_samples(&rolled).await?;
    Ok((
        u64::try_from(rolled.len()).unwrap_or(u64::MAX),
        2 + statements,
    ))
}

/// `minutes` (by process, then time) as one rollup per process at `at`: memory's averages
/// rounded and peaks, CPU's average and peak over the minutes that have one, the container's
/// latest limit, every role the window saw, the minutes it covers, and a worker's parts on
/// average over every minute (its largest tasks left out), so the 7 and 30 days show memory by
/// kind too.
#[must_use]
pub fn rolled_up(minutes: &[Sample], at: Timestamp) -> Vec<Sample> {
    // The window's minutes in which any process was sampled: a total's denominator.
    let covered = u32::try_from(
        minutes
            .iter()
            .map(|s| s.at)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
    )
    .unwrap_or(u32::MAX);
    let mut by_process: BTreeMap<&str, Vec<&Sample>> = BTreeMap::new();
    for sample in minutes {
        by_process
            .entry(&sample.process_id)
            .or_default()
            .push(sample);
    }
    by_process
        .into_iter()
        .map(|(process, samples)| {
            #[expect(clippy::cast_precision_loss, reason = "an average of byte counts")]
            let average = |values: &[u64]| {
                (!values.is_empty()).then(|| {
                    let sum: f64 = values.iter().map(|v| *v as f64).sum();
                    #[expect(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "a rounded average of byte counts: non-negative, in range"
                    )]
                    let rounded = (sum / values.len() as f64).round() as u64;
                    rounded
                })
            };
            let resident: Vec<u64> = samples.iter().map(|s| s.resident_avg).collect();
            let memory: Vec<u64> = samples.iter().map(|s| s.memory_avg).collect();
            let container: Vec<u64> = samples.iter().filter_map(|s| s.container_avg).collect();
            let cpu: Vec<f64> = samples.iter().filter_map(|s| s.cpu_avg).collect();
            #[expect(clippy::cast_precision_loss, reason = "a count of minutes")]
            let cpu_avg = (!cpu.is_empty()).then(|| cpu.iter().sum::<f64>() / cpu.len() as f64);
            Sample {
                process_id: process.to_owned(),
                step_s: ROLLUP,
                at,
                roles: samples
                    .iter()
                    .flat_map(|s| s.roles.iter().map(|(k, v)| (k.clone(), v.clone())))
                    .collect(),
                resident_avg: average(&resident).unwrap_or(0),
                resident_peak: samples.iter().map(|s| s.resident_peak).max().unwrap_or(0),
                memory_avg: average(&memory).unwrap_or(0),
                memory_peak: samples.iter().map(|s| s.memory_peak).max().unwrap_or(0),
                minutes: samples.iter().map(|s| s.minutes).sum(),
                cpu_minutes: samples.iter().map(|s| s.cpu_minutes).sum(),
                container_minutes: samples.iter().map(|s| s.container_minutes).sum(),
                covered,
                cpu_seconds: None,
                cpu_avg,
                cpu_peak: samples.iter().filter_map(|s| s.cpu_peak).reduce(f64::max),
                container_avg: average(&container),
                container_peak: samples.iter().filter_map(|s| s.container_peak).max(),
                // The latest minute's: none there is unlimited (Codex's review of M5.1j).
                container_limit: samples.last().and_then(|s| s.container_limit),
                // Each part over every minute, 0 where it ran no worker, so the parts and the
                // process itself add up to its whole memory (Codex's review of M5.1j).
                parts: {
                    let ran = samples.iter().any(|s| s.parts.is_some());
                    let part = |of: fn(&WorkerParts) -> u64| {
                        average(
                            &samples
                                .iter()
                                .map(|s| s.parts.as_ref().map_or(0, of))
                                .collect::<Vec<_>>(),
                        )
                        .unwrap_or(0)
                    };
                    ran.then(|| WorkerParts {
                        fork_servers_bytes: part(|p| p.fork_servers_bytes),
                        watcher_hosts_bytes: part(|p| p.watcher_hosts_bytes),
                        services_bytes: part(|p| p.services_bytes),
                        tasks_bytes: part(|p| p.tasks_bytes),
                        largest_tasks: Vec::new(),
                    })
                },
            }
        })
        .collect()
}

/// A webserver in a process of its own samples itself every `interval` (decision 464) on `db`
/// as `process_id`, its figures from `usage`, until `stop` completes; a failure is logged.
pub async fn sample_webserver(
    db: Database,
    interval: Duration,
    process_id: String,
    usage: fn() -> Option<NodeUsage>,
    stop: impl Future<Output = ()>,
) {
    tokio::pin!(stop);
    let mut sampler = Sampler::default();
    let roles: BTreeMap<String, String> =
        BTreeMap::from([("webserver".to_owned(), process_id.clone())]);
    loop {
        if let Some(usage) = usage() {
            let measured = Timestamp::now();
            let sample = sampler.sample(
                (process_id.clone(), roles.clone()),
                &usage,
                None,
                (floor(measured, MINUTE), measured),
            );
            if let Err(error) = db.insert_samples(&[sample]).await {
                tracing::warn!(%error, "this webserver's sample not stored; trying again in a minute");
            }
        }
        tokio::select! {
            () = &mut stop => return,
            () = tokio::time::sleep(interval) => {}
        }
    }
}

/// `duration` as a signed one, the longest when it doesn't fit.
fn within(duration: Duration) -> SignedDuration {
    SignedDuration::try_from(duration).unwrap_or(SignedDuration::MAX)
}
