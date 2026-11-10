//! The master's sender: delivers the outbox (decision 410). A notification delivered is
//! deleted; one that fails is tried again later, waiting longer each time, until
//! `[notifications] give_up_after` has passed since it was queued: then it is logged as an
//! error, counted and deleted. Redirects aren't followed (decision 425), and a URL is never
//! logged (a server webhook's may hold its secret).

use std::sync::Arc;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_db::notifications::Notification;
use schedr_db::{Database, DbError};

use super::Notifications;
use crate::metrics::MasterMetrics;

/// The most notifications one pass delivers.
pub const BATCH: u32 = 50;

/// The waits between deliveries of a failing notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// The first wait; it doubles.
    pub first: Duration,
    /// The longest.
    pub max: Duration,
}

impl Backoff {
    /// The wait after the `tries`-th failure (1 for the first).
    #[must_use]
    pub fn after(self, tries: u32) -> Duration {
        let factor = 1u32
            .checked_shl(tries.saturating_sub(1))
            .unwrap_or(u32::MAX);
        self.first.saturating_mul(factor).min(self.max)
    }
}

/// Delivers notifications.
#[derive(Debug, Clone)]
pub struct Sender {
    db: Database,
    settings: Arc<Notifications>,
    client: reqwest::Client,
    metrics: MasterMetrics,
    backoff: Backoff,
}

/// What a pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pass {
    /// Delivered.
    pub sent: usize,
    /// Failed, to be tried again.
    pub retried: usize,
    /// Given up on.
    pub dropped: usize,
}

impl Sender {
    /// A sender over `db`.
    ///
    /// # Errors
    ///
    /// The HTTP client can't be built (no TLS roots).
    pub fn new(
        db: Database,
        settings: Arc<Notifications>,
        metrics: MasterMetrics,
        backoff: Backoff,
    ) -> Result<Self, reqwest::Error> {
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(settings.timeout)
            .user_agent(concat!("schedr/", env!("CARGO_PKG_VERSION")));
        if let Some(pem) = &settings.extra_roots {
            builder = builder.tls_certs_merge(reqwest::Certificate::from_pem_bundle(pem)?);
        }
        let client = builder.build()?;
        Ok(Self {
            db,
            settings,
            client,
            metrics,
            backoff,
        })
    }

    /// Delivers what is due at `now`, at most [`BATCH`], while `leading` says this master still
    /// leads: it is asked before each delivery, so a master that lost its lease mid-pass starts
    /// no more (Codex's review of M4.13a).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the outbox can't be read or written.
    pub async fn pass(&self, now: Timestamp, leading: impl Fn() -> bool) -> Result<Pass, DbError> {
        let mut pass = Pass::default();
        for notification in self.db.due_notifications(now, BATCH).await? {
            if !leading() {
                break;
            }
            // Queued under older settings: a DAG webhook no longer allowed goes nowhere.
            if let Some(Err(why)) = notification
                .url
                .as_deref()
                .map(|url| self.settings.dag_webhook_allowed(url))
            {
                tracing::error!(
                    dag_id = notification.dag_id,
                    channel = notification.channel,
                    event = notification.event,
                    why,
                    "a notification to a DAG's webhook isn't allowed now; dropped"
                );
                self.db.delete_notification(&notification.id).await?;
                self.metrics.notification_failed(&notification.channel);
                pass.dropped += 1;
                continue;
            }
            match self.deliver(&notification).await {
                Ok(()) => {
                    self.db.delete_notification(&notification.id).await?;
                    self.metrics.notification_sent(&notification.channel);
                    pass.sent += 1;
                }
                Err(Failure::Refused(error)) => {
                    tracing::error!(
                        dag_id = notification.dag_id,
                        channel = notification.channel,
                        event = notification.event,
                        %error,
                        "a notification was refused by its receiver; dropped (a refusal isn't \
                         retried)"
                    );
                    self.db.delete_notification(&notification.id).await?;
                    self.metrics.notification_failed(&notification.channel);
                    pass.dropped += 1;
                }
                Err(Failure::Passing(error)) if self.given_up(&notification, now) => {
                    tracing::error!(
                        dag_id = notification.dag_id,
                        channel = notification.channel,
                        event = notification.event,
                        tries = notification.tries + 1,
                        %error,
                        "a notification couldn't be delivered within [notifications] \
                         give_up_after; dropped"
                    );
                    self.db.delete_notification(&notification.id).await?;
                    self.metrics.notification_failed(&notification.channel);
                    pass.dropped += 1;
                }
                Err(Failure::Passing(error))
                    if notification.url.is_none()
                        && !self.settings.servers.contains_key(&notification.channel) =>
                {
                    // An error, not a passing failure: kept until `give_up_after`, so a
                    // master started with the channel back delivers it.
                    tracing::error!(
                        dag_id = notification.dag_id,
                        channel = notification.channel,
                        event = notification.event,
                        %error,
                        "a notification names a server channel that isn't configured"
                    );
                    self.retry_later(&notification, now, &error).await?;
                    pass.retried += 1;
                }
                Err(Failure::Passing(error)) => {
                    tracing::warn!(
                        dag_id = notification.dag_id,
                        channel = notification.channel,
                        event = notification.event,
                        %error,
                        "a notification wasn't delivered; trying again in {}s",
                        self.backoff.after(notification.tries + 1).as_secs()
                    );
                    self.retry_later(&notification, now, &error).await?;
                    pass.retried += 1;
                }
            }
        }
        Ok(pass)
    }

    async fn retry_later(
        &self,
        notification: &Notification,
        now: Timestamp,
        error: &str,
    ) -> Result<(), DbError> {
        let wait = self.backoff.after(notification.tries + 1);
        let next = now
            .checked_add(SignedDuration::try_from(wait).unwrap_or(SignedDuration::MAX))
            .unwrap_or(now);
        self.db
            .retry_notification(&notification.id, next, error)
            .await
    }

    fn given_up(&self, notification: &Notification, now: Timestamp) -> bool {
        let limit =
            SignedDuration::try_from(self.settings.give_up_after).unwrap_or(SignedDuration::MAX);
        notification
            .created_at
            .checked_add(limit)
            .is_ok_and(|deadline| now >= deadline)
    }

    /// Posts `notification`.
    async fn deliver(&self, notification: &Notification) -> Result<(), Failure> {
        let (url, headers) = if let Some(url) = &notification.url {
            (url.as_str(), &[][..])
        } else {
            let server = self
                .settings
                .servers
                .get(&notification.channel)
                .ok_or_else(|| {
                    Failure::Passing(format!(
                        "no server channel `{}` in the configuration any more",
                        notification.channel
                    ))
                })?;
            (server.url.as_str(), server.headers.as_slice())
        };
        let mut request = self.client.post(url).json(&notification.body);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let answer = request.send().await.map_err(|error| {
            // Without the URL: reqwest's message names it.
            let error = error.without_url();
            Failure::Passing(if error.is_timeout() {
                format!("no answer within {}s", self.settings.timeout.as_secs())
            } else {
                error.to_string()
            })
        })?;
        let status = answer.status();
        if status.is_success() {
            Ok(())
        } else if status.is_client_error()
            && status != reqwest::StatusCode::REQUEST_TIMEOUT
            && status != reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            // It won't fix itself (decision 428).
            Err(Failure::Refused(format!("HTTP {status}")))
        } else {
            Err(Failure::Passing(format!("HTTP {status}")))
        }
    }
}

/// How a delivery failed.
#[derive(Debug)]
enum Failure {
    /// A 4xx but 408 and 429: dropped at once.
    Refused(String),
    /// Anything else: tried again until `give_up_after`.
    Passing(String),
}
