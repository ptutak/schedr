//! What an attempt reports beyond its result (decision 513): its status, each kept with its
//! time, and its links. Masked as its output is, bounded, and told to a sink (the agent's
//! loop, which sends them to the master) as they come. Metrics go to the worker's
//! per-extension figures instead.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use schedr_model::extension::{EventKind, ExtensionEvent, MAX_LINKS, MAX_TEXT, link_url_allowed};

use crate::mask::MaskSet;

/// The most statuses an attempt keeps: past it, the oldest after the first go, so the first
/// phase and the latest ones stay.
pub const MAX_STATUSES: usize = 100;

/// One status an attempt reported.
#[derive(Debug, Clone, PartialEq)]
pub struct Status {
    /// When the worker got it.
    pub at: jiff::Timestamp,
    /// Its phase.
    pub phase: String,
    /// More about it.
    pub message: Option<String>,
    /// How far along, 0 to 1.
    pub progress: Option<f64>,
}

/// One link an attempt reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    /// What it links to.
    pub label: String,
    /// Its URL, `http` or `https`.
    pub url: String,
}

/// What a report changed, for the sink.
#[derive(Debug, Clone, PartialEq)]
pub enum Report {
    /// A status, appended.
    Status(Status),
    /// A link, added or replacing the one with its label.
    Link(Link),
}

/// Told each report as it is kept.
#[derive(Clone)]
pub struct ReportSink(pub Arc<dyn Fn(&Report) + Send + Sync>);

/// The same sink: the same function.
impl PartialEq for ReportSink {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl std::fmt::Debug for ReportSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReportSink")
    }
}

/// Why a report isn't kept.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refused {
    /// The phase or label is empty.
    #[error("its {0} is empty")]
    Empty(&'static str),
    /// The URL isn't `http` or `https`, or is too long.
    #[error("a link's URL is http or https, at most {MAX_TEXT} characters")]
    Url,
    /// Its links are at their limit and this label is new.
    #[error("an attempt keeps at most {MAX_LINKS} links")]
    TooManyLinks,
    /// A notification its plugin didn't declare (M5.2i8): said once per name an attempt.
    #[error("notification `{0}` isn't one its plugin declared: dropped")]
    Undeclared(String),
    /// The same undeclared notification again: dropped without a word.
    #[error("an undeclared notification again")]
    UndeclaredAgain,
}

/// A plugin's notification kept as its extension's event once the master accepted it (agy's
/// review of M5.2i8: kept before, a refused one showed as sent).
#[derive(Debug)]
pub struct Notified {
    figures: crate::figures::Figures,
    event: ExtensionEvent,
}

impl Notified {
    /// Keeps it on its extension's page.
    pub fn record(self) {
        self.figures.event(self.event);
    }
}

/// The task API's route a plugin's notification goes through (the master's `NOTIFICATIONS`).
pub const NOTIFICATIONS_PATH: &str = "/api/v1/notifications";

#[derive(Debug, Default)]
struct Kept {
    statuses: Vec<Status>,
    links: Vec<Link>,
    declared: schedr_model::extension::Declarations,
    /// How many reports may be told now, and when it was last counted.
    tokens: f64,
    counted: Option<std::time::Instant>,
    /// The newest status past the rate, told with the next one or at the end.
    held: Option<Status>,
    /// Whether a link past the rate was warned of.
    warned: bool,
    /// The undeclared notifications already warned of.
    undeclared: std::collections::BTreeSet<String>,
    /// The sender of the plugin's notifications, once one was sent (M5.2i8): waited for before
    /// the attempt's end is told, as the master takes them only from an open attempt.
    notifying: Option<tokio::task::JoinHandle<()>>,
}

/// Reports told on per second, and at most at once (Codex's review of M5.2i5: a plugin
/// reporting faster than the master takes them filled the worker's queues): past them, a
/// status waits as the newest (one, replaced) and a link is dropped.
/// The defaults of `SCHEDR_HIDDEN__WORKER__EXTENSION_REPORTS_PER_SECOND` and `..._AT_ONCE`.
pub const REPORTS_PER_SECOND: usize = 10;
/// The burst.
pub const REPORTS_AT_ONCE: usize = 50;

/// An extension attempt's metrics: where they go and what its config declares.
#[derive(Debug, Clone)]
pub struct Figured {
    /// The worker's figures.
    pub figures: crate::figures::Figures,
    /// The extension.
    pub extension: String,
    /// Its version.
    pub version: String,
    /// Its `[extensions.<name>]` declarations, winning over its hello's where set.
    pub configured: schedr_model::extension::Declarations,
    /// The most series each metric keeps.
    pub max_series: u32,
}

/// An attempt's reports.
#[derive(Debug)]
pub struct AttemptReports {
    mask: Arc<MaskSet>,
    sink: Option<ReportSink>,
    figured: Option<Figured>,
    stop: crate::supervise::StopNote,
    kept: Mutex<Kept>,
    /// Reports a second, and at once.
    rate: (f64, f64),
}

/// A count of reports as the rate's figure: at most 16,777,216, a hidden limit's most, exact.
#[expect(
    clippy::cast_precision_loss,
    reason = "a hidden limit is at most 2^24: exact in f64"
)]
const fn rate_of(count: usize) -> f64 {
    count as f64
}

/// `declarations`' texts masked: what a plugin's `hello` declared may name a secret the attempt
/// read (agy's review of M5.2i5). Names stay: they are checked identifiers.
fn masked_declarations(mask: &MaskSet, declarations: &mut schedr_model::extension::Declarations) {
    for metric in &mut declarations.metrics {
        metric.help = mask.mask_text(&metric.help);
        for label in &mut metric.labels {
            for value in label.values.iter_mut().flatten() {
                *value = mask.mask_text(value);
            }
        }
    }
    for event in &mut declarations.notifications {
        event.help = mask.mask_text(&event.help);
    }
    for panel in &mut declarations.panels {
        panel.title = mask.mask_text(&panel.title);
        panel.colors = std::mem::take(&mut panel.colors)
            .into_iter()
            .map(|(value, color)| (mask.mask_text(&value), color))
            .collect();
    }
}

/// `text` masked and cut to [`MAX_TEXT`] characters.
fn bounded(mask: &MaskSet, text: &str) -> String {
    mask.mask_text(text.trim()).chars().take(MAX_TEXT).collect()
}

impl AttemptReports {
    /// An attempt's reports, masked with `mask`.
    #[must_use]
    pub fn new(mask: Arc<MaskSet>) -> Self {
        Self {
            mask,
            sink: None,
            figured: None,
            stop: crate::supervise::StopNote::default(),
            kept: Mutex::default(),
            rate: (rate_of(REPORTS_PER_SECOND), rate_of(REPORTS_AT_ONCE)),
        }
    }

    /// The same, told at most `per_second` reports a second, `at_once` at once.
    #[must_use]
    pub fn limited(mut self, per_second: usize, at_once: usize) -> Self {
        self.rate = (rate_of(per_second.max(1)), rate_of(at_once.max(1)));
        self
    }

    /// The same, each report told to `sink` once kept.
    #[must_use]
    pub fn forwarded(mut self, sink: ReportSink) -> Self {
        self.sink = Some(sink);
        self
    }

    /// The same, its metrics kept in `figured`'s figures.
    #[must_use]
    pub fn figured(mut self, figured: Figured) -> Self {
        self.figured = Some(figured);
        self
    }

    /// Keeps `points` in the worker's figures: one that its declarations don't allow is
    /// dropped, warned of once per metric.
    pub fn metrics(&self, points: &[schedr_model::extension::MetricPoint]) {
        let Some(figured) = &self.figured else {
            return;
        };
        for point in points {
            // Its label values masked as its statuses are (agy's review of M5.2i5: a secret the
            // attempt read reached the figures, the master and the page).
            let mut point = point.clone();
            for value in point.labels.values_mut() {
                *value = self.mask.mask_text(value);
            }
            if let Err(why) = figured
                .figures
                .observe(&figured.extension, &figured.version, &point)
                && figured
                    .figures
                    .warn_once(&figured.extension, &figured.version, &point.name)
            {
                let why = self
                    .mask
                    .mask_text(&format!("a metric point dropped: {why}"));
                tracing::warn!(
                    extension = figured.extension,
                    version = figured.version,
                    "{why}"
                );
                figured.figures.event(ExtensionEvent::now(
                    &figured.extension,
                    Some(&figured.version),
                    EventKind::Dropped,
                    &why,
                ));
            }
        }
    }

    /// Where its supervision writes why it is stopped, for `stop_reason_get`.
    #[must_use]
    pub fn stop_note(&self) -> crate::supervise::StopNote {
        self.stop.clone()
    }

    /// Why it is being stopped, as a plugin is told (decision 513), and the grace left;
    /// none while it isn't, or for a memory kill (SIGKILL at once, no grace).
    #[must_use]
    pub fn stop_reason(&self) -> (Option<schedr_model::extension::StopKind>, Duration) {
        use crate::status::StopReason;
        use schedr_model::extension::StopKind;
        match self.stop.read() {
            Some((reason, left)) => {
                let kind = match reason {
                    StopReason::Cancelled => Some(StopKind::Cancel),
                    StopReason::TimedOut => Some(StopKind::Timeout),
                    StopReason::Interrupted => Some(StopKind::Drain),
                    StopReason::CutOff => Some(StopKind::MasterLost),
                    StopReason::OomKilled => None,
                };
                (kind, if kind.is_some() { left } else { Duration::ZERO })
            }
            None => (None, Duration::ZERO),
        }
    }

    fn tell(&self, report: &Report) {
        let Some(sink) = &self.sink else {
            return;
        };
        let held = {
            let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
            let now = std::time::Instant::now();
            let since = kept.counted.map_or(self.rate.1 / self.rate.0, |at| {
                now.duration_since(at).as_secs_f64()
            });
            kept.tokens = since.mul_add(self.rate.0, kept.tokens).min(self.rate.1);
            kept.counted = Some(now);
            if kept.tokens < 1.0 {
                match report {
                    Report::Status(status) => kept.held = Some(status.clone()),
                    Report::Link(_) if !kept.warned => {
                        kept.warned = true;
                        tracing::warn!(
                            "an attempt's links past {} reports a second: dropped",
                            self.rate.0
                        );
                    }
                    Report::Link(_) => {}
                }
                return;
            }
            kept.tokens -= 1.0;
            // The one held is older than this: told only if this isn't a newer status.
            kept.held
                .take()
                .filter(|_| !matches!(report, Report::Status(_)))
        };
        if let Some(held) = held {
            (sink.0)(&Report::Status(held));
        }
        (sink.0)(report);
    }

    /// Keeps the task sending the plugin's notifications (M5.2i8), for [`Self::notified_by`].
    pub fn notifying(&self, sender: tokio::task::JoinHandle<()>) {
        self.kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notifying = Some(sender);
    }

    /// Waits, at most until `limit`, for the plugin's notifications still on their way: the
    /// master takes one only while its attempt is open, so they go before the attempt's end is
    /// told (a side note on agy's review of M5.2i8: sent in the background, the last one, sent
    /// just before the plugin ended, lost the race). Past `limit` they go on, maybe refused.
    pub async fn notified_by(&self, limit: Duration) {
        let sender = self
            .kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .notifying
            .take();
        if let Some(sender) = sender
            && tokio::time::timeout(limit, sender).await.is_err()
        {
            tracing::warn!(
                "a plugin's notifications still on their way after {limit:?}: its attempt's end \
                 is told; they may be refused"
            );
        }
    }

    /// Tells the status held back past the rate, if any: at the attempt's end, so its last
    /// phase is the one shown.
    pub fn flush(&self) {
        let held = self
            .kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .held
            .take();
        if let (Some(sink), Some(held)) = (&self.sink, held) {
            (sink.0)(&Report::Status(held));
        }
    }

    /// What a plugin's `notify` sends the master (M5.2i8): one of its declared events, its text
    /// masked and bounded; the body of its call to [`NOTIFICATIONS_PATH`] (its data masked by
    /// the caller with the request), and its record as an event of its extension (DN19), kept
    /// once the master accepted it.
    ///
    /// # Errors
    ///
    /// [`Refused::Undeclared`] the first time an undeclared event is sent,
    /// [`Refused::UndeclaredAgain`] after.
    pub fn notification(
        &self,
        event: &str,
        text: Option<&str>,
        data: Option<serde_json::Value>,
    ) -> Result<(serde_json::Value, Option<Notified>), Refused> {
        let text = text
            .map(|text| bounded(&self.mask, text))
            .filter(|t| !t.is_empty());
        {
            let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
            if !kept.declared.notifications.iter().any(|n| n.name == event) {
                // Bounded (agy's review): past MAX_WARNED names, dropped without a word.
                if kept.undeclared.len() >= crate::figures::MAX_WARNED
                    || !kept.undeclared.insert(event.to_owned())
                {
                    return Err(Refused::UndeclaredAgain);
                }
                drop(kept);
                let why = Refused::Undeclared(bounded(&self.mask, event));
                if let Some(figured) = &self.figured {
                    figured.figures.event(ExtensionEvent::now(
                        &figured.extension,
                        Some(&figured.version),
                        EventKind::Dropped,
                        &why.to_string(),
                    ));
                }
                return Err(why);
            }
        }
        let notified = self.figured.as_ref().map(|figured| {
            let said = text
                .as_deref()
                .map_or_else(|| event.to_owned(), |t| format!("{event}: {t}"));
            Notified {
                figures: figured.figures.clone(),
                event: ExtensionEvent::now(
                    &figured.extension,
                    Some(&figured.version),
                    EventKind::Notified,
                    &said,
                ),
            }
        });
        let body = serde_json::json!({
            "extension_event": event,
            "text": text,
            "data": data.unwrap_or(serde_json::Value::Null),
        });
        Ok((body, notified))
    }

    /// Keeps a status, masked and bounded; a progress outside 0 to 1 is clamped, one not a
    /// number dropped.
    ///
    /// # Errors
    ///
    /// [`Refused::Empty`] for an empty phase.
    pub fn status(
        &self,
        phase: &str,
        message: Option<&str>,
        progress: Option<f64>,
    ) -> Result<(), Refused> {
        if phase.trim().is_empty() {
            return Err(Refused::Empty("phase"));
        }
        let status = Status {
            at: jiff::Timestamp::now(),
            phase: bounded(&self.mask, phase),
            message: message
                .filter(|m| !m.trim().is_empty())
                .map(|m| bounded(&self.mask, m)),
            progress: progress.filter(|p| !p.is_nan()).map(|p| p.clamp(0.0, 1.0)),
        };
        {
            let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
            if kept.statuses.len() >= MAX_STATUSES {
                kept.statuses.remove(1);
            }
            kept.statuses.push(status.clone());
        }
        self.tell(&Report::Status(status));
        Ok(())
    }

    /// Keeps a link, replacing one with its label.
    ///
    /// # Errors
    ///
    /// [`Refused`]: an empty label, a URL not `http` or `https`, or one link too many.
    pub fn link(&self, label: &str, url: &str) -> Result<(), Refused> {
        if label.trim().is_empty() {
            return Err(Refused::Empty("label"));
        }
        if !link_url_allowed(url) {
            return Err(Refused::Url);
        }
        let link = Link {
            label: bounded(&self.mask, label),
            // Masked too: a URL may carry a token.
            url: self.mask.mask_text(url.trim()),
        };
        {
            let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(same) = kept.links.iter_mut().find(|l| l.label == link.label) {
                same.url.clone_from(&link.url);
            } else if kept.links.len() >= MAX_LINKS {
                return Err(Refused::TooManyLinks);
            } else {
                kept.links.push(link.clone());
            }
        }
        self.tell(&Report::Link(link));
        Ok(())
    }

    /// Keeps what the plugin's `hello` declared (decision 513), its config's metrics and
    /// panels winning where set; an extension's are its worker's figures' declarations.
    pub fn declared(&self, declared: crate::channel::Declared) {
        let crate::channel::Declared {
            mut declarations,
            dropped,
        } = declared;
        masked_declarations(&self.mask, &mut declarations);
        if let Some(figured) = &self.figured {
            if let Some(why) = dropped {
                figured.figures.event(ExtensionEvent::now(
                    &figured.extension,
                    Some(&figured.version),
                    EventKind::Dropped,
                    &self
                        .mask
                        .mask_text(&format!("its declarations not used: {why}")),
                ));
            }
            if !figured.configured.metrics.is_empty() {
                declarations.metrics.clone_from(&figured.configured.metrics);
            }
            if !figured.configured.panels.is_empty() {
                declarations.panels.clone_from(&figured.configured.panels);
            }
            if !figured.configured.notifications.is_empty() {
                declarations
                    .notifications
                    .clone_from(&figured.configured.notifications);
            }
            figured.figures.declare(
                &figured.extension,
                &figured.version,
                declarations.clone(),
                figured.max_series,
            );
        }
        self.kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .declared = declarations;
    }

    /// Its plugin's `hello` refused, for `why`: an event of its extension (DN19).
    pub fn refused(&self, why: &str) {
        if let Some(figured) = &self.figured {
            figured.figures.event(ExtensionEvent::now(
                &figured.extension,
                Some(&figured.version),
                EventKind::Refused,
                &self
                    .mask
                    .mask_text(&format!("its plugin's hello refused: {why}")),
            ));
        }
    }

    /// What the plugin's `hello` declared; empty for any other task.
    #[must_use]
    pub fn declarations(&self) -> schedr_model::extension::Declarations {
        self.kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .declared
            .clone()
    }

    /// The statuses kept, oldest first.
    #[must_use]
    pub fn statuses(&self) -> Vec<Status> {
        self.kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .statuses
            .clone()
    }

    /// The links kept, in the order first reported.
    #[must_use]
    pub fn links(&self) -> Vec<Link> {
        self.kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .links
            .clone()
    }
}

#[cfg(test)]
mod tests;
