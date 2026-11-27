//! Extensions' reports off the master's round (decision 521): a worker's heartbeat (its
//! extensions' figures and events) and its attempts' statuses and links are handed to a bounded
//! queue, never waited on, and a writer of their own stores them a flush at a time, each write
//! with a short time limit. A full queue, a failed write or a slow one drops them, counted
//! (`master_extension_reports_dropped`) and warned of at most once a minute: extensions rank
//! below native tasks, and their reports never slow the master.

use schedr_model::hold::HoldId;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::time::{Duration, Instant};

use jiff::Timestamp;
use schedr_db::extension_samples::{ExtensionSample, SeenVersion};
use schedr_db::reports::{AttemptLink, AttemptStatus, ReportedLink, ReportedStatus};
use schedr_db::{Database, DbError};
use schedr_proto::pb;
use tokio::sync::{mpsc, oneshot};

use crate::figures::SeriesSeen;
use crate::metrics::MasterMetrics;

#[cfg(test)]
mod tests;

/// How often an extension version's seen time is written at most, its declarations unchanged
/// (decision 521): the page says "seen a minute ago" at best.
pub const SEEN_EVERY: Duration = Duration::from_mins(1);

/// How often a kind of drop is warned of at most.
const WARN_EVERY: Duration = Duration::from_mins(1);

/// The writer's limits (`SCHEDR_HIDDEN__MASTER__EXTENSION_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReportSettings {
    /// The reports held at most; past them, dropped.
    pub queue: usize,
    /// Writes at most this often: what comes meanwhile joins the next.
    pub flush_interval: Duration,
    /// The longest a write may take; past it, its reports are dropped.
    pub write_timeout: Duration,
    /// The events of one extension a flush keeps.
    pub events_per_flush: usize,
    /// The records of one kind a worker's report keeps (`MASTER__WORKER_REPORT_RECORDS`).
    pub records: usize,
    /// The events kept per extension (`MASTER__EXTENSION_EVENTS_KEPT`).
    pub events_kept: usize,
}

impl Default for ReportSettings {
    fn default() -> Self {
        Self {
            queue: 10_000,
            flush_interval: Duration::from_secs(1),
            write_timeout: Duration::from_secs(2),
            events_per_flush: 100,
            records: 256,
            events_kept: 200,
        }
    }
}

/// What a worker reported of its extensions.
#[derive(Debug)]
pub enum Report {
    /// A heartbeat's figures (decision 513), as sent.
    Figures {
        /// The worker.
        worker_id: String,
        /// Each extension's figures, JSON.
        sent: Vec<Vec<u8>>,
        /// When the master heard them.
        now: Timestamp,
    },
    /// A heartbeat's events (DN19), as sent.
    Events {
        /// The worker.
        worker_id: String,
        /// Each event, JSON.
        sent: Vec<Vec<u8>>,
        /// When the master heard them.
        now: Timestamp,
    },
    /// A running attempt's status or link.
    Attempt {
        /// The worker.
        worker_id: String,
        /// What it reported.
        reported: pb::AttemptReported,
    },
    /// Answered once what was queued before it is written (tests, and a clean stop).
    Flushed(oneshot::Sender<()>),
}

/// The queue's sending end: [`ExtensionReports::offer`] never waits.
#[derive(Debug, Clone)]
pub struct ExtensionReports {
    sender: mpsc::Sender<Report>,
    metrics: MasterMetrics,
}

impl ExtensionReports {
    /// Starts the writer over `db` (on the current runtime) and returns its queue.
    #[must_use]
    pub fn start(db: Database, settings: ReportSettings, metrics: MasterMetrics) -> Self {
        let (sender, receiver) = mpsc::channel(settings.queue.max(1));
        let writer = Writer::new(db, settings, metrics.clone());
        tokio::spawn(writer.run(receiver));
        Self { sender, metrics }
    }

    /// Queues `report`; dropped, counted and warned of, when the queue is full.
    pub fn offer(&self, report: Report) {
        match self.sender.try_send(report) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.count_extension_dropped("full", 1);
                warn_rarely(
                    "full",
                    "extensions' reports dropped: the master's queue is full",
                );
            }
            // The writer is gone (the runtime stopping): nothing to keep them for.
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.count_extension_dropped("failed", 1);
            }
        }
    }

    /// Waits until what was queued before this call is written (or dropped).
    pub async fn flushed(&self) {
        let (told, answer) = oneshot::channel();
        if self.sender.send(Report::Flushed(told)).await.is_ok() {
            let _ = answer.await;
        }
    }
}

/// Warns of a drop of `kind`, at most once a [`WARN_EVERY`] per kind across the process.
fn warn_rarely(kind: &'static str, message: &str) {
    static LAST: std::sync::Mutex<BTreeMap<&'static str, Instant>> =
        std::sync::Mutex::new(BTreeMap::new());
    let now = Instant::now();
    let due = {
        let mut last = LAST
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match last.get(kind) {
            Some(at) if now.duration_since(*at) < WARN_EVERY => false,
            _ => {
                last.insert(kind, now);
                true
            }
        }
    };
    if due {
        tracing::warn!(
            reason = kind,
            "{message} (decision 521; counted in master_extension_reports_dropped)"
        );
    }
}

/// The writer: a flush at a time, each part one transaction within the time limit.
struct Writer {
    db: Database,
    settings: ReportSettings,
    metrics: MasterMetrics,
    /// The extensions' series stored this hour, across workers (decision 513): past a metric's
    /// cap, a new one is stored as `other`.
    series_seen: SeriesSeen,
    /// When each extension version of each worker was last written as seen.
    seen_at: HashMap<(String, String, String), Instant>,
}

/// A flush's reports, sorted by kind.
#[derive(Default)]
struct Flush {
    figures: Vec<(String, Vec<Vec<u8>>, Timestamp)>,
    events: Vec<(String, Vec<Vec<u8>>, Timestamp)>,
    /// The statuses, in the order heard; of an attempt's consecutive ones with the same phase,
    /// the newest (decision 521: its phase changes kept, its progress ticks coalesced).
    statuses: Vec<ReportedStatus>,
    /// Where each attempt's last status from each worker is in `statuses`: keyed by its worker
    /// too, as its owner is checked only when stored (agy's review: another worker's report
    /// replaced the owner's in the flush, and neither was kept).
    last_status: BTreeMap<(String, String), usize>,
    /// The newest URL of each attempt's label, from each worker.
    links: BTreeMap<(String, String, String), ReportedLink>,
    /// Attempt reports in this flush, counted when dropped.
    attempt_reports: u64,
    flushed: Vec<oneshot::Sender<()>>,
}

impl Writer {
    fn new(db: Database, settings: ReportSettings, metrics: MasterMetrics) -> Self {
        Self {
            db,
            settings,
            metrics,
            series_seen: SeriesSeen::default(),
            seen_at: HashMap::new(),
        }
    }

    async fn run(mut self, mut receiver: mpsc::Receiver<Report>) {
        let mut last: Option<tokio::time::Instant> = None;
        while let Some(first) = receiver.recv().await {
            // At most one write a flush interval: what comes meanwhile joins it.
            if let Some(last) = last {
                tokio::time::sleep_until(last + self.settings.flush_interval).await;
            }
            let mut batch = vec![first];
            while batch.len() < self.settings.queue {
                match receiver.try_recv() {
                    Ok(report) => batch.push(report),
                    Err(_) => break,
                }
            }
            last = Some(tokio::time::Instant::now());
            Box::pin(self.write(batch)).await;
        }
    }

    async fn write(&mut self, batch: Vec<Report>) {
        let mut flush = Flush::default();
        for report in batch {
            match report {
                Report::Figures {
                    worker_id,
                    sent,
                    now,
                } => flush.figures.push((worker_id, sent, now)),
                Report::Events {
                    worker_id,
                    sent,
                    now,
                } => flush.events.push((worker_id, sent, now)),
                Report::Attempt {
                    worker_id,
                    reported,
                } => {
                    flush.attempt_reports += 1;
                    attempt_report(&mut flush, worker_id, reported);
                }
                Report::Flushed(told) => flush.flushed.push(told),
            }
        }
        if !flush.statuses.is_empty() || !flush.links.is_empty() {
            let statuses = std::mem::take(&mut flush.statuses);
            let links: Vec<ReportedLink> = std::mem::take(&mut flush.links).into_values().collect();
            let db = self.db.clone();
            self.timed("reports", flush.attempt_reports, async move {
                db.add_attempt_reports(&statuses, &links).await.map(|_| ())
            })
            .await;
        }
        let figures = std::mem::take(&mut flush.figures);
        if !figures.is_empty() {
            self.figures(figures).await;
        }
        let events = std::mem::take(&mut flush.events);
        if !events.is_empty() {
            self.events(events).await;
        }
        for told in flush.flushed {
            let _ = told.send(());
        }
    }

    /// Runs `write` within the time limit, timed as `query`; on failure or past the limit,
    /// `count` reports are dropped. Whether it was stored.
    async fn timed(
        &self,
        query: &str,
        count: u64,
        write: impl Future<Output = Result<(), DbError>> + Send,
    ) -> bool {
        let began = Instant::now();
        // Quietly, as the master's other bookkeeping writes (agy's review): a change signal
        // wakes the trigger loop, and these writes change nothing it reads.
        // Boxed: each write's future is large, and the writer's would carry them all.
        let done = tokio::time::timeout(
            self.settings.write_timeout,
            Box::pin(self.db.quietly(write)),
        )
        .await;
        self.metrics.extension_query_done(query, began.elapsed());
        match done {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                self.metrics.count_extension_dropped("failed", count);
                warn_rarely(
                    "failed",
                    &format!("extensions' reports dropped: {query} not written: {error}"),
                );
                false
            }
            Err(_) => {
                self.metrics.count_extension_dropped("slow", count);
                warn_rarely(
                    "slow",
                    &format!(
                        "extensions' reports dropped: {query} took over {:?}",
                        self.settings.write_timeout
                    ),
                );
                false
            }
        }
    }

    /// The flush's figures: each extension version's seen time (its declarations when sent;
    /// otherwise at most once a [`SEEN_EVERY`]) in one transaction, and their minute samples,
    /// within each metric's series cap across workers, merged in another.
    async fn figures(&mut self, figures: Vec<(String, Vec<Vec<u8>>, Timestamp)>) {
        let count = u64::try_from(figures.len()).unwrap_or(u64::MAX);
        let mut seen: Vec<SeenVersion> = Vec::new();
        let mut samples: BTreeMap<SampleKey, ExtensionSample> = BTreeMap::new();
        // The series read this flush, stored or not: a failed one waits for the next flush,
        // not read again for each worker (agy's review: a slow table times out once per worker).
        let mut read: std::collections::BTreeSet<(String, String)> =
            std::collections::BTreeSet::new();
        let mut latest = Timestamp::UNIX_EPOCH;
        for (worker_id, sent, now) in figures {
            latest = latest.max(now);
            let at = Timestamp::from_second(now.as_second().div_euclid(60) * 60).unwrap_or(now);
            for json in sent.iter().take(crate::figures::MAX_EXTENSIONS) {
                let figures = match serde_json::from_slice(json)
                    .map_err(|e| e.to_string())
                    .and_then(crate::figures::checked)
                {
                    Ok(figures) => figures,
                    Err(why) => {
                        tracing::warn!(worker_id, "extension figures not kept: {why}");
                        continue;
                    }
                };
                let key = (
                    figures.extension.clone(),
                    figures.version.clone(),
                    worker_id.clone(),
                );
                let declared = figures
                    .declarations
                    .clone()
                    .map(|d| (d, figures.max_series));
                let due = self
                    .seen_at
                    .get(&key)
                    .is_none_or(|at| at.elapsed() >= SEEN_EVERY);
                if declared.is_some() || due {
                    self.seen_at.insert(key, Instant::now());
                    seen.push(SeenVersion {
                        name: figures.extension.clone(),
                        version: figures.version.clone(),
                        worker_id: worker_id.clone(),
                        declared,
                    });
                }
                let mut minute = crate::figures::minute_samples(&figures, &worker_id, at);
                let (hour, unloaded) = self.series_seen.unloaded(&minute, at);
                for (name, metric) in unloaded {
                    if !read.insert((name.clone(), metric.clone())) {
                        continue;
                    }
                    let began = Instant::now();
                    let stored = tokio::time::timeout(
                        self.settings.write_timeout,
                        self.db.extension_series_since(&name, &metric, hour),
                    )
                    .await;
                    self.metrics.extension_query_done("series", began.elapsed());
                    match stored {
                        Ok(Ok(stored)) => self.series_seen.load((name, metric), stored),
                        Ok(Err(error)) => {
                            tracing::warn!(worker_id, %error, "an extension's series not read");
                        }
                        Err(_) => tracing::warn!(worker_id, "an extension's series read too slow"),
                    }
                }
                let dropped = self.series_seen.fold(&mut minute, figures.max_series, at);
                if dropped > 0 {
                    tracing::warn!(
                        worker_id,
                        extension = figures.extension,
                        "{dropped} series dropped: label names not the hour's"
                    );
                }
                merge_heartbeat(&mut samples, minute);
            }
        }
        let samples: Vec<ExtensionSample> = samples.into_values().collect();
        self.store_figures(seen, samples, latest, count).await;
    }

    /// A flush's figures stored: the versions' seen times, then their minute samples.
    async fn store_figures(
        &mut self,
        seen: Vec<SeenVersion>,
        samples: Vec<ExtensionSample>,
        latest: Timestamp,
        count: u64,
    ) {
        if !seen.is_empty() {
            let db = self.db.clone();
            let written = self
                .timed("seen", count, async move {
                    db.extensions_seen(&seen, latest).await
                })
                .await;
            if !written {
                // Written again with the next figures, not a minute later.
                self.seen_at.clear();
            }
        }
        if !samples.is_empty() {
            let folded: std::collections::BTreeSet<(String, String)> = samples
                .iter()
                .map(|s| (s.name.clone(), s.metric.clone()))
                .collect();
            let db = self.db.clone();
            let written = self
                .timed("minutes", count, async move {
                    db.add_extension_minutes(&samples).await
                })
                .await;
            if !written {
                // Not stored: their series read again before the next fold (Grok's review).
                self.series_seen.forget(&folded);
            }
        }
    }

    /// The flush's events, each worker's newest within its records, at most
    /// `events_per_flush` of each extension (the rest dropped), in one transaction.
    async fn events(&self, events: Vec<(String, Vec<Vec<u8>>, Timestamp)>) {
        let count = u64::try_from(events.len()).unwrap_or(u64::MAX);
        let mut kept = Vec::new();
        let mut per_extension: BTreeMap<String, usize> = BTreeMap::new();
        let mut capped = 0_u64;
        for (worker_id, sent, now) in events {
            for event in crate::figures::newest_events(&sent, self.settings.records, now) {
                let n = per_extension.entry(event.extension.clone()).or_default();
                if *n >= self.settings.events_per_flush {
                    capped += 1;
                    continue;
                }
                *n += 1;
                let mut event = event;
                event.message = storable(&event.message);
                event.version = event.version.as_deref().map(storable);
                kept.push((worker_id.clone(), event));
            }
        }
        if capped > 0 {
            self.metrics.count_extension_dropped("many", capped);
            warn_rarely(
                "many",
                "extensions' events dropped: more of one extension than a flush keeps",
            );
        }
        if kept.is_empty() {
            return;
        }
        let db = self.db.clone();
        let most = self.settings.events_kept;
        self.timed("events", count, async move {
            db.insert_workers_extension_events(&kept, most).await
        })
        .await;
    }
}

/// An attempt's report, checked and cut as from any worker, into the flush: each phase change
/// (consecutive statuses of one phase coalesced to the newest), the newest URL of each label. Its owner is checked when it is stored.
fn attempt_report(flush: &mut Flush, worker_id: String, reported: pb::AttemptReported) {
    // An attempt's UUID or a hold of it (decision 535): another would fail the flush's whole
    // transaction on Postgres (agy's review), so it is dropped here, as its owner check would
    // drop it. Kept under the ID it came with, so two holds' reports never coalesce; the
    // store keeps it under its attempt, from its current hold only.
    if !HoldId::is_valid(&reported.attempt_id) {
        return;
    }
    let cut = |text: String| -> String { storable(&text) };
    match reported.report {
        Some(pb::attempt_reported::Report::Status(status)) if !status.phase.is_empty() => {
            let status = AttemptStatus {
                at: Timestamp::from_millisecond(status.at_ms).unwrap_or_else(|_| Timestamp::now()),
                phase: cut(status.phase),
                message: status.message.map(cut),
                progress: status
                    .progress
                    .filter(|p| !p.is_nan())
                    .map(|p| p.clamp(0.0, 1.0)),
            };
            let reported = ReportedStatus {
                attempt_id: reported.attempt_id,
                worker_id,
                extension: cut(reported.extension),
                version: cut(reported.extension_version),
                status,
            };
            // The same phase again: the newest replaces it, in its place.
            let key = (reported.attempt_id.clone(), reported.worker_id.clone());
            let same_phase = flush
                .last_status
                .get(&key)
                .copied()
                .filter(|&at| flush.statuses[at].status.phase == reported.status.phase);
            if let Some(at) = same_phase {
                flush.statuses[at] = reported;
            } else {
                flush.last_status.insert(key, flush.statuses.len());
                flush.statuses.push(reported);
            }
        }
        Some(pb::attempt_reported::Report::Link(link))
            if !link.label.is_empty() && schedr_model::extension::link_url_allowed(&link.url) =>
        {
            let label = cut(link.label);
            flush.links.insert(
                (
                    reported.attempt_id.clone(),
                    worker_id.clone(),
                    label.clone(),
                ),
                ReportedLink {
                    attempt_id: reported.attempt_id,
                    worker_id,
                    link: AttemptLink {
                        label,
                        url: storable(&link.url),
                    },
                },
            );
        }
        _ => {}
    }
}

/// `text` cut to what a report keeps, with no NUL: Postgres refuses one in a text, and the
/// flush's whole transaction with it (agy's review).
fn storable(text: &str) -> String {
    text.chars()
        .take(schedr_model::extension::MAX_TEXT)
        .map(|c| if c == '\0' { '\u{fffd}' } else { c })
        .collect()
}

/// A minute sample's identity: its extension, version, worker, minute, metric and series.
type SampleKey = (
    String,
    String,
    String,
    i64,
    String,
    BTreeMap<String, String>,
);

/// One heartbeat's minute samples into the flush's: its own series folded into one are added
/// (their growth both counts), a series another heartbeat of the flush sent already is merged
/// as a later figure (agy's review: a worker's gauge sent twice in a minute was summed).
fn merge_heartbeat(
    samples: &mut BTreeMap<SampleKey, ExtensionSample>,
    minute: Vec<ExtensionSample>,
) {
    let mut own: BTreeMap<SampleKey, ExtensionSample> = BTreeMap::new();
    for sample in minute {
        let key = (
            sample.name.clone(),
            sample.version.clone(),
            sample.worker_id.clone(),
            sample.at.as_second(),
            sample.metric.clone(),
            sample.labels.clone(),
        );
        match own.remove(&key) {
            Some(mut earlier) => {
                earlier.data = earlier.data.added(sample.data);
                own.insert(key, earlier);
            }
            None => {
                own.insert(key, sample);
            }
        }
    }
    for (key, sample) in own {
        match samples.remove(&key) {
            Some(mut earlier) => {
                earlier.data = earlier.data.merged(sample.data);
                samples.insert(key, earlier);
            }
            None => {
                samples.insert(key, sample);
            }
        }
    }
}
