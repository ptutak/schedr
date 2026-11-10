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

use lettre::AsyncTransport as _;
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::client::{Certificate, Tls, TlsParameters};
use lettre::{AsyncSmtpTransport, Message, Tokio1Executor};
use schedr_config::config::SmtpTls;

use super::{KIND_EMAIL, Notifications, Smtp};
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
    /// The mail server's transport, when `[notifications.smtp]` has a host.
    smtp: Option<AsyncSmtpTransport<Tokio1Executor>>,
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
    /// The HTTP client or the mail server's transport can't be built (no TLS roots, a bad
    /// `ca_file`).
    pub fn new(
        db: Database,
        settings: Arc<Notifications>,
        metrics: MasterMetrics,
        backoff: Backoff,
    ) -> Result<Self, String> {
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(settings.timeout)
            .user_agent(concat!("schedr/", env!("CARGO_PKG_VERSION")));
        if let Some(pem) = &settings.extra_roots {
            let roots = reqwest::Certificate::from_pem_bundle(pem).map_err(|e| e.to_string())?;
            builder = builder.tls_certs_merge(roots);
        }
        let client = builder.build().map_err(|e| e.to_string())?;
        let smtp = settings
            .smtp
            .as_ref()
            .map(|smtp| transport(smtp, settings.extra_roots.as_deref(), settings.timeout))
            .transpose()
            .map_err(|e| format!("[notifications.smtp]: {e}"))?;
        Ok(Self {
            db,
            settings,
            client,
            metrics,
            backoff,
            smtp,
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
                        && !self.settings.servers.contains_key(&notification.channel)
                        && !self.settings.emails.contains_key(&notification.channel) =>
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

    /// Sends `notification`: posts it, or mails it.
    async fn deliver(&self, notification: &Notification) -> Result<(), Failure> {
        // The channel as configured now: its recipients may have changed or gone since it
        // was queued, and a channel missing then may be back as an email one (Codex's review
        // of M4.13b).
        if notification.url.is_none()
            && let Some(channel) = self.settings.emails.get(&notification.channel)
        {
            return self.mail(notification, &channel.to).await;
        }
        if notification.kind == KIND_EMAIL {
            return Err(Failure::Passing(format!(
                "no email channel `{}` in the configuration any more",
                notification.channel
            )));
        }
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

impl Sender {
    /// Mails `notification` to `to`: its body holds the email (`subject`, `text`), or, if it
    /// was queued while its channel was missing, schedr's JSON.
    async fn mail(&self, notification: &Notification, to: &[String]) -> Result<(), Failure> {
        let (Some(transport), Some(smtp)) = (&self.smtp, &self.settings.smtp) else {
            return Err(Failure::Passing(
                "no [notifications.smtp] host in the configuration any more".to_owned(),
            ));
        };
        let message = email(&smtp.from, to, &notification.body).map_err(Failure::Refused)?;
        // A deadline over the whole exchange: lettre's own covers only connecting, and a server
        // that never answers would hold up every notification behind it (Codex's review of
        // M4.13b). Cut after the server took the message, it's sent again: at least once.
        let timeout = self.settings.timeout;
        match tokio::time::timeout(timeout, transport.send(message)).await {
            Err(_) => Err(Failure::Passing(format!(
                "no answer within {}s",
                timeout.as_secs()
            ))),
            Ok(Ok(_)) => Ok(()),
            // A 5xx won't fix itself (decision 428, as a webhook's 4xx).
            Ok(Err(error)) if error.is_permanent() => Err(Failure::Refused(error.to_string())),
            Ok(Err(error)) => Err(Failure::Passing(error.to_string())),
        }
    }
}

/// The message to `to` of an email notification's body: its `subject` and `text`, or schedr's
/// JSON (queued while its channel was missing), its `text` as the subject and all of it below.
fn email(from: &str, to: &[String], body: &serde_json::Value) -> Result<Message, String> {
    let field = |key: &str| body.get(key).and_then(serde_json::Value::as_str);
    let (subject, text) = if let (Some(subject), Some(text)) = (field("subject"), field("text")) {
        (subject.to_owned(), text.to_owned())
    } else {
        let summary = field("text").unwrap_or("A notification from schedr");
        let all = serde_json::to_string_pretty(body).unwrap_or_default();
        (super::subject_of(summary), format!("{summary}\n\n{all}\n"))
    };
    let from = from.parse().map_err(|e| format!("from: {e}"))?;
    let mut builder = Message::builder().from(from).subject(subject);
    for address in to {
        builder = builder.to(address
            .parse()
            .map_err(|e| format!("to `{address}`: {e}"))?);
    }
    builder
        .header(lettre::message::header::ContentType::TEXT_PLAIN)
        .body(text)
        .map_err(|e| e.to_string())
}

/// The mail server's transport: TLS as `[notifications.smtp] tls` says, trusting the system's
/// roots and `ca_file`'s; the login, if any.
fn transport(
    smtp: &Smtp,
    extra_roots: Option<&[u8]>,
    timeout: Duration,
) -> Result<AsyncSmtpTransport<Tokio1Executor>, lettre::transport::smtp::Error> {
    let tls = if smtp.tls == SmtpTls::None {
        Tls::None
    } else {
        let mut parameters = TlsParameters::builder(smtp.host.clone());
        if let Some(pem) = extra_roots {
            parameters = parameters.add_root_certificate(Certificate::from_pem(pem)?);
        }
        let parameters = parameters.build()?;
        if smtp.tls == SmtpTls::Tls {
            Tls::Wrapper(parameters)
        } else {
            Tls::Required(parameters)
        }
    };
    let mut builder = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&smtp.host)
        .port(smtp.port)
        .tls(tls)
        .timeout(Some(timeout));
    if let Some((user, password)) = &smtp.login {
        builder = builder.credentials(Credentials::new(user.clone(), password.clone()));
    }
    Ok(builder.build())
}

/// How a delivery failed.
#[derive(Debug)]
enum Failure {
    /// A 4xx but 408 and 429: dropped at once.
    Refused(String),
    /// Anything else: tried again until `give_up_after`.
    Passing(String),
}
