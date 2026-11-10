//! Notifications (decisions 408-429): which channels hear of an event, the message each gets,
//! and the checks on a DAG's `notify=`.
//!
//! An event queues one notification per channel told of it, in the transaction that records
//! the event (the outbox, decision 410); [`sender`] delivers them. A body is filled here, when
//! it is queued, from fixed values ([`Placeholder`]): nothing in it is code.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use jiff::Timestamp;
use schedr_config::config::{ChannelKind, NotificationsSection, SmtpSection, SmtpTls};
use schedr_db::notifications::{FailedTask, NewNotification};
use schedr_model::notify::{Body, ChannelRef, NotifyEvent, Part, PathKey, Placeholder, Ref};
use schedr_model::{Dag, Trigger};
use serde_json::{Map, Value, json};

use crate::metrics::MasterMetrics;

pub mod sender;
#[cfg(test)]
mod tests;

/// The event name of a message a task sends itself.
pub const MESSAGE: &str = "MESSAGE";

/// A notification posted to a webhook (`notifications.kind`).
pub const KIND_WEBHOOK: &str = "webhook";

/// A notification sent as an email; its body holds `to`, `subject` and `text`.
pub const KIND_EMAIL: &str = "email";

/// The most bytes a filled body may take by default (1 MiB).
pub const DEFAULT_MAX_BODY: usize = 1 << 20;

/// The most failed tasks a message lists.
pub const MAX_FAILED_TASKS: u32 = 20;

/// The notification settings (`[notifications]`), checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notifications {
    /// The UI's address, without a trailing `/`; none: messages carry no links.
    pub ui_url: Option<String>,
    /// The events a channel named without events is told of.
    pub default_events: Vec<NotifyEvent>,
    /// The server's webhooks, by name.
    pub servers: BTreeMap<String, ServerWebhook>,
    /// The server's email channels, by name (decision 426).
    pub emails: BTreeMap<String, EmailChannel>,
    /// The mail server they send through; none: no email channel is configured.
    pub smtp: Option<Smtp>,
    /// Where a DAG's own webhook may post: its URL starts with one of them; none: DAGs define
    /// no webhooks (decision 425).
    pub dag_urls: Vec<String>,
    /// Extra CAs trusted for deliveries, PEM (`[notifications] ca_file`, read by the caller).
    pub extra_roots: Option<Vec<u8>>,
    /// The limit for one delivery.
    pub timeout: Duration,
    /// How long a notification is retried.
    pub give_up_after: Duration,
    /// The most notifications a task attempt may send itself.
    pub max_per_attempt: u32,
    /// The most bytes a filled body may take (`SCHEDR_HIDDEN__MASTER__NOTIFICATION_BODY`).
    pub max_body: usize,
}

/// A server webhook, its headers read from the environment.
#[derive(Clone, PartialEq, Eq)]
pub struct ServerWebhook {
    /// Where it posts (it may be its secret: never logged).
    pub url: String,
    /// Its headers, name and value (values are secrets: never logged).
    pub headers: Vec<(String, String)>,
    /// Its body; none: schedr's fixed JSON.
    pub body: Option<Body>,
}

impl std::fmt::Debug for ServerWebhook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.headers.iter().map(|(name, _)| name.as_str()).collect();
        f.debug_struct("ServerWebhook")
            .field("headers", &names)
            .field("body", &self.body.is_some())
            .finish_non_exhaustive()
    }
}

/// A server email channel (`type = "email"`): its recipients, every one in To.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmailChannel {
    /// The recipients, checked.
    pub to: Vec<String>,
}

/// The mail server (`[notifications.smtp]`), checked.
#[derive(Clone, PartialEq, Eq)]
pub struct Smtp {
    /// Its host.
    pub host: String,
    /// Its port.
    pub port: u16,
    /// How the connection is secured.
    pub tls: SmtpTls,
    /// The login, user and password, if any (the password is a secret: never logged).
    pub login: Option<(String, String)>,
    /// The sender.
    pub from: String,
}

impl std::fmt::Debug for Smtp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Smtp")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("user", &self.login.as_ref().map(|(user, _)| user))
            .field("from", &self.from)
            .finish_non_exhaustive()
    }
}

impl Default for Notifications {
    fn default() -> Self {
        Self::from_config(&NotificationsSection::default(), &BTreeMap::new())
            .unwrap_or_else(|_| unreachable!("the default settings are valid"))
    }
}

impl Notifications {
    /// The settings of `section`, header values read from `env`.
    ///
    /// # Errors
    ///
    /// A message naming the key: an unknown default event, a URL that isn't `http(s)://`, a
    /// header's variable that isn't set, or a body with a bad placeholder.
    pub fn from_config(
        section: &NotificationsSection,
        env: &BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let default_events = section
            .default_events
            .iter()
            .map(|name| {
                NotifyEvent::parse(name).ok_or_else(|| {
                    let known: Vec<&str> = NotifyEvent::ALL.iter().map(|e| e.name()).collect();
                    format!(
                        "[notifications] default_events: unknown event `{name}` (known: {})",
                        known.join(", ")
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let smtp = smtp_of(&section.smtp)?;
        let mut servers = BTreeMap::new();
        let mut emails = BTreeMap::new();
        for (name, channel) in &section.channels {
            let key = format!("[notifications.channels.{name}]");
            if channel.kind == ChannelKind::Email {
                if smtp.is_none() {
                    return Err(format!(
                        "{key}: an email channel sends through [notifications.smtp], which has \
                         no `host`"
                    ));
                }
                emails.insert(name.clone(), email_channel(&key, channel)?);
                continue;
            }
            if !channel.to.is_empty() {
                return Err(format!(
                    "{key} to: a webhook has no recipients (`to` is an email's)"
                ));
            }
            let url = channel
                .url
                .as_ref()
                .ok_or_else(|| format!("{key}: a webhook needs its `url`"))?;
            check_url(url).map_err(|e| format!("{key} url: {e}"))?;
            let headers = channel
                .headers_env
                .iter()
                .map(|(header, variable)| {
                    env.get(variable)
                        .filter(|value| !value.is_empty())
                        .map(|value| (header.clone(), value.clone()))
                        .ok_or_else(|| {
                            format!(
                                "{key} headers_env: `{header}` is read from the environment \
                                 variable {variable}, which isn't set"
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let body = channel
                .body
                .as_ref()
                .map(Body::from_config)
                .transpose()
                .map_err(|e| format!("{key} body: {e}"))?;
            servers.insert(
                name.clone(),
                ServerWebhook {
                    url: url.clone(),
                    headers,
                    body,
                },
            );
        }
        for url in &section.dag_urls {
            check_url(url).map_err(|e| format!("[notifications] dag_urls: {e}"))?;
            let parsed = reqwest::Url::parse(url)
                .map_err(|e| format!("[notifications] dag_urls: `{url}` doesn't read: {e}"))?;
            if !parsed.username().is_empty() || parsed.password().is_some() {
                return Err(format!(
                    "[notifications] dag_urls: `{url}` holds a user or a password"
                ));
            }
        }
        if let Some(url) = &section.ui_url {
            check_url(url).map_err(|e| format!("[notifications] ui_url: {e}"))?;
        }
        Ok(Self {
            ui_url: section
                .ui_url
                .as_ref()
                .map(|url| url.trim_end_matches('/').to_owned()),
            default_events,
            servers,
            emails,
            smtp,
            dag_urls: section.dag_urls.clone(),
            extra_roots: None,
            timeout: section.timeout,
            give_up_after: section.give_up_after,
            max_per_attempt: section.max_per_attempt,
            max_body: DEFAULT_MAX_BODY,
        })
    }

    /// What is wrong with `dag`'s `notify=`: a server channel that isn't configured, a DAG
    /// channel where they are off, outside `dag_urls`, named as a server channel, or two of
    /// one name.
    #[must_use]
    pub fn problems(&self, dag: &Dag) -> Vec<String> {
        let mut problems = Vec::new();
        let mut names = std::collections::BTreeSet::new();
        for entry in &dag.notify {
            let name = entry.channel.name();
            if !names.insert(name) {
                problems.push(format!("notify: two channels are named `{name}`"));
            }
            match &entry.channel {
                ChannelRef::Server { name } if !self.is_server_channel(name) => {
                    problems.push(format!(
                        "notify: no server channel `{name}` ([notifications.channels.{name}] in \
                         the master's configuration; configured: {})",
                        self.server_names()
                    ));
                }
                ChannelRef::Server { .. } => {}
                ChannelRef::Webhook(hook) => {
                    if self.dag_webhook_allowed(&hook.url).is_err() {
                        problems.push(format!(
                            "notify: WebhookChannel `{name}`: its URL isn't one DAGs may post to \
                             ([notifications] dag_urls: {}); a server channel can be named \
                             instead",
                            list(self.dag_urls.iter())
                        ));
                    }
                    if self.is_server_channel(name) {
                        problems.push(format!(
                            "notify: WebhookChannel `{name}` is named as a server channel; give \
                             it another name"
                        ));
                    }
                }
            }
        }
        problems
    }

    /// The notifications `event` queues: one per channel of the DAG told of it. A missing
    /// value fills as null. A DAG webhook whose URL is off `dag_urls` now, or a body past its
    /// size, is logged as an error, counted in `metrics` as failed and skipped; a server
    /// channel not configured now is queued (the sender keeps it until `give_up_after`).
    #[must_use]
    pub fn queued(
        &self,
        event: &Event<'_>,
        kind: NotifyEvent,
        metrics: &MasterMetrics,
    ) -> Vec<NewNotification> {
        let vars = self.vars(event, kind.name());
        event
            .dag
            .notify
            .iter()
            .filter(|entry| {
                entry
                    .events
                    .as_ref()
                    .map_or(self.default_events.contains(&kind), |events| {
                        events.contains(&kind)
                    })
            })
            .filter_map(|entry| {
                self.notification(event, &entry.channel, kind.name(), &vars, false)
                    .map_err(|error| {
                        tracing::error!(dag_id = %event.dag.id, channel = entry.channel.name(),
                            event = kind.name(), %error, "a notification can't be sent; dropped");
                        metrics.notification_failed(entry.channel.name());
                    })
                    .ok()
            })
            .collect()
    }

    /// The notification a task's own `send` queues to channel `name` of its DAG (its own
    /// channels first, then the server's).
    ///
    /// # Errors
    ///
    /// A message for the caller: no such channel, or a path into `data` the data lacks.
    pub fn message(&self, event: &Event<'_>, name: &str) -> Result<NewNotification, String> {
        let own = event
            .dag
            .notify
            .iter()
            .map(|entry| &entry.channel)
            .find(|channel| channel.name() == name);
        let server = ChannelRef::Server {
            name: name.to_owned(),
        };
        let channel = match own {
            Some(channel) => channel,
            None if self.is_server_channel(name) => &server,
            None => {
                return Err(format!(
                    "no channel `{name}`: not in DAG `{}`'s notify=, nor a server channel \
                     (configured: {})",
                    event.dag.id,
                    self.server_names()
                ));
            }
        };
        let vars = self.vars(event, MESSAGE);
        self.notification(event, channel, MESSAGE, &vars, true)
    }

    /// Whether `name` is a channel of the server's configuration, of either type.
    fn is_server_channel(&self, name: &str) -> bool {
        self.servers.contains_key(name) || self.emails.contains_key(name)
    }

    /// The server's channels' names, for messages.
    fn server_names(&self) -> String {
        let names: std::collections::BTreeSet<&String> =
            self.servers.keys().chain(self.emails.keys()).collect();
        list(names.into_iter())
    }

    fn notification(
        &self,
        happened: &Event<'_>,
        channel: &ChannelRef,
        event: &str,
        vars: &Vars,
        strict: bool,
    ) -> Result<NewNotification, String> {
        let dag = happened.dag;
        if let ChannelRef::Server { name } = channel
            && self.emails.contains_key(name)
        {
            let body = email_json(happened, event, vars);
            Budget::new(self.max_body).spend(json_len(&body))?;
            return Ok(NewNotification {
                dag_id: dag.id.to_string(),
                channel: name.clone(),
                kind: KIND_EMAIL.to_owned(),
                url: None,
                event: event.to_owned(),
                body,
            });
        }
        let (url, body) = match channel {
            // Not configured now: queued all the same, with schedr's JSON; the sender keeps it
            // until `give_up_after`, so a master started with the channel back delivers it
            // (decision 425; Codex's review of M4.13a).
            ChannelRef::Server { name } => (
                None,
                self.servers
                    .get(name)
                    .and_then(|server| server.body.as_ref()),
            ),
            ChannelRef::Webhook(hook) => {
                self.dag_webhook_allowed(&hook.url)?;
                (Some(hook.url.clone()), hook.body.as_ref())
            }
        };
        let body = if let Some(body) = body {
            fill(body, vars, strict, &mut Budget::new(self.max_body))?
        } else {
            // A task's data is in it whole: held to the same limit (Codex's docs round).
            let body = fixed(vars);
            Budget::new(self.max_body).spend(json_len(&body))?;
            body
        };
        Ok(NewNotification {
            dag_id: dag.id.to_string(),
            channel: channel.name().to_owned(),
            kind: KIND_WEBHOOK.to_owned(),
            url,
            event: event.to_owned(),
            body,
        })
    }

    /// Whether a DAG's own webhook may post to `url` under the settings now (they may have
    /// changed since its DAG was checked).
    ///
    /// # Errors
    ///
    /// Why not: `url` outside `dag_urls`.
    pub fn dag_webhook_allowed(&self, url: &str) -> Result<(), String> {
        if self.dag_urls.iter().any(|allowed| url_within(url, allowed)) {
            Ok(())
        } else {
            Err("its URL isn't one DAGs may post to any more ([notifications] dag_urls)".to_owned())
        }
    }

    fn vars(&self, event: &Event<'_>, name: &str) -> Vars {
        let link = match (&self.ui_url, event.run_id) {
            (Some(base), Some(run)) => Value::String(format!("{base}/runs/{run}")),
            (Some(base), None) => Value::String(format!("{base}/dags/{}", event.dag.id)),
            (None, _) => Value::Null,
        };
        let failed: Vec<Value> = event
            .failed_tasks
            .iter()
            .map(|t| {
                let task = if t.map_index >= 0 {
                    format!("{}[{}]", t.task_id, t.map_index)
                } else {
                    t.task_id.clone()
                };
                json!({"task": task, "error": t.error})
            })
            .collect();
        let text = event.text.clone().unwrap_or_else(|| summary(event, name));
        let text = match &link {
            Value::String(link) if event.text.is_none() => format!("{text} {link}"),
            _ => text,
        };
        BTreeMap::from([
            (Placeholder::Event, Value::String(name.to_owned())),
            (Placeholder::Dag, Value::String(event.dag.id.to_string())),
            (Placeholder::Run, opt(event.run_id)),
            (
                Placeholder::PlannedFor,
                event
                    .planned_for
                    .map_or(Value::Null, |t| Value::String(t.to_string())),
            ),
            (Placeholder::State, opt(event.state)),
            (Placeholder::Text, Value::String(text)),
            (Placeholder::Link, link),
            (Placeholder::FailedTasks, Value::Array(failed)),
            (Placeholder::Task, opt(event.task)),
            (
                Placeholder::Attempt,
                event.attempt.map_or(Value::Null, Value::from),
            ),
            (Placeholder::Time, Value::String(event.time.to_string())),
            (Placeholder::Zone, Value::String(zone(event.dag).to_owned())),
            (Placeholder::Data, event.data.clone()),
        ])
    }
}

/// What happened, for the message.
#[derive(Debug, Clone)]
pub struct Event<'a> {
    /// The DAG.
    pub dag: &'a Dag,
    /// The run, if any (not for a failing trigger).
    pub run_id: Option<&'a str>,
    /// The run's planned time.
    pub planned_for: Option<Timestamp>,
    /// The run's or the task's state.
    pub state: Option<&'a str>,
    /// The task, for task events and `send`.
    pub task: Option<&'a str>,
    /// The attempt's number, for task events and `send`.
    pub attempt: Option<u32>,
    /// The failed tasks.
    pub failed_tasks: Vec<FailedTask>,
    /// The error, for a failing trigger or a task event.
    pub error: Option<String>,
    /// A task's own text (`send`); none: schedr's summary.
    pub text: Option<String>,
    /// A task's own data (`send`); null otherwise.
    pub data: Value,
    /// When it happened.
    pub time: Timestamp,
}

impl<'a> Event<'a> {
    /// An event of `dag` at `time`, nothing else known yet.
    #[must_use]
    pub const fn new(dag: &'a Dag, time: Timestamp) -> Self {
        Self {
            dag,
            run_id: None,
            planned_for: None,
            state: None,
            task: None,
            attempt: None,
            failed_tasks: Vec::new(),
            error: None,
            text: None,
            data: Value::Null,
            time,
        }
    }
}

type Vars = BTreeMap<Placeholder, Value>;

fn opt(value: Option<&str>) -> Value {
    value.map_or(Value::Null, |v| Value::String(v.to_owned()))
}

fn list<'a>(names: impl Iterator<Item = &'a String>) -> String {
    let names: Vec<String> = names.map(|n| format!("`{n}`")).collect();
    if names.is_empty() {
        "none".to_owned()
    } else {
        names.join(", ")
    }
}

/// Whether `url` is within `allowed` (decision 425): the same scheme, host and port, no user or
/// password, and a path under `allowed`'s, at a `/` (`https://a.example.com/hooks` allows
/// `/hooks` and `/hooks/x`, not `/hooksy`). Both are parsed, as the client parses them, so a
/// host suffix (`a.example.com.evil`) or userinfo (`a.example.com@127.0.0.1`) can't pass.
#[must_use]
pub fn url_within(url: &str, allowed: &str) -> bool {
    let (Ok(url), Ok(allowed)) = (reqwest::Url::parse(url), reqwest::Url::parse(allowed)) else {
        return false;
    };
    if !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    let same_origin = url.scheme() == allowed.scheme()
        && url.host_str().is_some()
        && url.host_str() == allowed.host_str()
        && url.port_or_known_default() == allowed.port_or_known_default();
    let base = allowed.path();
    let path = url.path();
    same_origin
        && (base.ends_with('/') && path.starts_with(base)
            || path == base
            || path
                .strip_prefix(base)
                .is_some_and(|rest| rest.starts_with('/')))
}

fn check_url(url: &str) -> Result<(), String> {
    if url.starts_with("https://") || url.starts_with("http://") {
        Ok(())
    } else {
        Err(format!("`{url}` must start with https:// or http://"))
    }
}

/// The DAG's time zone: its schedule's, UTC otherwise.
fn zone(dag: &Dag) -> &str {
    match &dag.trigger {
        Trigger::Cron { timezone, .. } => timezone,
        _ => "UTC",
    }
}

/// schedr's one-line summary of an event.
fn summary(event: &Event<'_>, name: &str) -> String {
    let dag = &event.dag.id;
    let run = event.run_id.unwrap_or("?");
    let task = event.task.unwrap_or("?");
    let error = event
        .error
        .as_deref()
        .map(|e| format!(": {}", first_line(e)))
        .unwrap_or_default();
    let failed = || {
        let names: Vec<String> = event
            .failed_tasks
            .iter()
            .map(|t| {
                if t.map_index >= 0 {
                    format!("{}[{}]", t.task_id, t.map_index)
                } else {
                    t.task_id.clone()
                }
            })
            .collect();
        if names.is_empty() {
            String::new()
        } else {
            format!(" (failed: {})", names.join(", "))
        }
    };
    match NotifyEvent::parse(name) {
        Some(NotifyEvent::RunFailed) => format!("DAG `{dag}`: run {run} failed{}", failed()),
        Some(NotifyEvent::TaskFailed) => {
            format!("DAG `{dag}`: task `{task}` failed in run {run}{error}")
        }
        Some(NotifyEvent::TaskRetry) => format!(
            "DAG `{dag}`: task `{task}` failed in run {run} (attempt {}) and will be retried{error}",
            event.attempt.unwrap_or(0)
        ),
        Some(NotifyEvent::RunCancelled) => format!("DAG `{dag}`: run {run} was cancelled"),
        Some(NotifyEvent::RunSucceeded) => format!("DAG `{dag}`: run {run} succeeded"),
        Some(NotifyEvent::RunRecovered) => {
            format!("DAG `{dag}`: run {run} succeeded after the previous run failed")
        }
        Some(NotifyEvent::TriggerFailing) => format!("DAG `{dag}`: its trigger is failing{error}"),
        Some(NotifyEvent::LateCleanupFailed) => format!(
            "DAG `{dag}`: a cleanup of task `{task}` failed after run {run} succeeded; handles \
             may be left behind{error}"
        ),
        None => format!("DAG `{dag}`: a message from task `{task}`"),
    }
}

fn first_line(text: &str) -> &str {
    let line = text.lines().next().unwrap_or_default();
    match line.char_indices().nth(300) {
        Some((at, _)) => &line[..at],
        None => line,
    }
}

/// schedr's fixed JSON (decision 412): every value, `data` only when set.
fn fixed(vars: &Vars) -> Value {
    let mut out = Map::new();
    for (placeholder, value) in vars {
        if *placeholder == Placeholder::Data && value.is_null() {
            continue;
        }
        out.insert(placeholder.name().to_owned(), value.clone());
    }
    Value::Object(out)
}

/// What a body may still take, in bytes of its JSON, as it is filled: each value is measured
/// before it is copied, so a template using a big value many times stops at the limit instead
/// of filling the master's memory (Codex's review of M4.13a).
#[derive(Debug)]
pub struct Budget {
    left: usize,
    max: usize,
}

impl Budget {
    /// `max` bytes.
    #[must_use]
    pub const fn new(max: usize) -> Self {
        Self { left: max, max }
    }

    fn spend(&mut self, bytes: usize) -> Result<(), String> {
        self.left = self.left.checked_sub(bytes).ok_or_else(|| {
            format!(
                "the body would take more than {} bytes \
                 (SCHEDR_HIDDEN__MASTER__NOTIFICATION_BODY)",
                self.max
            )
        })?;
        Ok(())
    }
}

/// The bytes of `value`'s JSON, counted without writing it.
fn json_len(value: &Value) -> usize {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    let _ = serde_json::to_writer(&mut count, value);
    count.0
}

/// `body` filled from `vars`, within `budget`. With `strict` (a task's own `send`), a path into
/// `data` that the data lacks is an error; otherwise it fills as null (empty in text).
///
/// # Errors
///
/// Past the budget; with `strict`, a path into `data` that isn't there. The message says which.
pub fn fill(body: &Body, vars: &Vars, strict: bool, budget: &mut Budget) -> Result<Value, String> {
    Ok(match body {
        Body::Value(value) => {
            budget.spend(json_len(value))?;
            value.clone()
        }
        Body::Ref(reference) => {
            let value = resolve(reference, vars, strict)?;
            budget.spend(json_len(value))?;
            value.clone()
        }
        Body::Template(parts) => {
            let mut text = String::new();
            for part in parts {
                match part {
                    Part::Text(piece) => {
                        budget.spend(piece.len())?;
                        text.push_str(piece);
                    }
                    Part::Ref(reference) => match resolve(reference, vars, strict)? {
                        Value::Null => {}
                        Value::String(value) => {
                            budget.spend(value.len())?;
                            text.push_str(value);
                        }
                        other => {
                            budget.spend(json_len(other))?;
                            text.push_str(&other.to_string());
                        }
                    },
                }
            }
            Value::String(text)
        }
        Body::Object(map) => Value::Object(
            map.iter()
                .map(|(key, body)| {
                    budget.spend(key.len())?;
                    Ok((key.clone(), fill(body, vars, strict, budget)?))
                })
                .collect::<Result<_, String>>()?,
        ),
        Body::Array(items) => Value::Array(
            items
                .iter()
                .map(|body| fill(body, vars, strict, budget))
                .collect::<Result<_, _>>()?,
        ),
    })
}

static NULL: Value = Value::Null;

fn resolve<'a>(reference: &Ref, vars: &'a Vars, strict: bool) -> Result<&'a Value, String> {
    let mut value = vars.get(&reference.name).unwrap_or(&NULL);
    for key in &reference.path {
        let next = match (key, value) {
            (PathKey::Key(key), Value::Object(map)) => map.get(key),
            (PathKey::Index(index), Value::Array(items)) => {
                usize::try_from(*index).ok().and_then(|i| items.get(i))
            }
            _ => None,
        };
        match next {
            Some(next) => value = next,
            None if strict && reference.name == Placeholder::Data => {
                return Err(format!(
                    "the channel's body uses `{reference}`, which the data sent hasn't"
                ));
            }
            None => return Ok(&NULL),
        }
    }
    Ok(value)
}

/// `[notifications.smtp]` checked: none without a host.
fn smtp_of(section: &SmtpSection) -> Result<Option<Smtp>, String> {
    let Some(host) = section.host.clone().filter(|host| !host.is_empty()) else {
        return Ok(None);
    };
    let from = section
        .from
        .clone()
        .ok_or_else(|| "[notifications.smtp]: `from`, the sender, is needed".to_owned())?;
    from.parse::<lettre::message::Mailbox>()
        .map_err(|e| format!("[notifications.smtp] from: `{from}` isn't an address: {e}"))?;
    let login = match (&section.username, &section.password) {
        (Some(user), Some(password)) => Some((user.clone(), password.clone())),
        (None, None) => None,
        (Some(_), None) => {
            return Err("[notifications.smtp] username: its password is set in \
                 SCHEDR_CONFIG__NOTIFICATIONS__SMTP__PASSWORD, which isn't set"
                .to_owned());
        }
        (None, Some(_)) => {
            return Err(
                "[notifications.smtp]: a password with no `username` to log in as".to_owned(),
            );
        }
    };
    if section.tls == SmtpTls::None && login.is_some() {
        return Err(
            "[notifications.smtp]: a login over `tls = \"none\"` would send the password in \
             plain text; use `starttls` or `tls`"
                .to_owned(),
        );
    }
    Ok(Some(Smtp {
        host,
        port: section.port,
        tls: section.tls,
        login,
        from,
    }))
}

/// A `type = "email"` channel checked: `to` only, every address one.
fn email_channel(
    key: &str,
    channel: &schedr_config::config::ChannelConfig,
) -> Result<EmailChannel, String> {
    if channel.url.is_some() || !channel.headers_env.is_empty() || channel.body.is_some() {
        return Err(format!(
            "{key}: an email channel takes `to` only (`url`, `headers_env` and `body` are a \
             webhook's)"
        ));
    }
    if channel.to.is_empty() {
        return Err(format!("{key} to: an email channel needs its recipients"));
    }
    for address in &channel.to {
        address
            .parse::<lettre::message::Mailbox>()
            .map_err(|e| format!("{key} to: `{address}` isn't an address: {e}"))?;
    }
    Ok(EmailChannel {
        to: channel.to.clone(),
    })
}

/// An email's subject: one line of at most 200 characters.
pub(crate) fn subject_of(text: &str) -> String {
    let line = text.lines().next().unwrap_or_default().trim();
    let cut: String = line.chars().take(200).collect();
    if cut.len() < line.len() {
        format!("{cut}…")
    } else {
        cut
    }
}

/// The email for `happened` (decision 426): plain text, the summary as the subject, then the
/// run, the task, the failed tasks with their errors, the times, a task's data and the link.
fn email_json(happened: &Event<'_>, event: &str, vars: &Vars) -> Value {
    let summary = happened
        .text
        .clone()
        .unwrap_or_else(|| summary(happened, event));
    let mut text = format!("{summary}\n\n");
    let mut line = |name: &str, value: &str| {
        let _ = writeln!(text, "{name:<13} {value}");
    };
    line("Event:", event);
    line("DAG:", happened.dag.id.as_str());
    if let Some(run) = happened.run_id {
        line("Run:", run);
    }
    let zone = zone(happened.dag);
    if let Some(planned) = happened.planned_for {
        line("Planned for:", &both_times(planned, zone));
    }
    if let Some(task) = happened.task {
        match happened.attempt {
            Some(attempt) => line("Task:", &format!("{task} (attempt {attempt})")),
            None => line("Task:", task),
        }
    }
    if let Some(error) = &happened.error {
        line("Error:", first_line(error));
    }
    line("At:", &both_times(happened.time, zone));
    if let Some(Value::String(link)) = vars.get(&Placeholder::Link) {
        line("Link:", link);
    }
    if !happened.failed_tasks.is_empty() {
        text.push_str("\nFailed tasks:\n");
        for task in &happened.failed_tasks {
            let name = if task.map_index >= 0 {
                format!("{}[{}]", task.task_id, task.map_index)
            } else {
                task.task_id.clone()
            };
            let _ = writeln!(
                text,
                "  {name}: {}",
                task.error
                    .as_deref()
                    .map_or("(no error recorded)", first_line)
            );
        }
    }
    if !happened.data.is_null() {
        let data = serde_json::to_string_pretty(&happened.data).unwrap_or_default();
        let _ = write!(text, "\nData:\n{data}\n");
    }
    json!({"subject": subject_of(&summary), "text": text})
}

/// `2026-11-09T03:00:00Z (04:00 Europe/Warsaw)`: UTC, and the DAG's zone when it isn't UTC.
fn both_times(at: Timestamp, zone: &str) -> String {
    if zone == "UTC" {
        return at.to_string();
    }
    match at.in_tz(zone) {
        Ok(local) => format!("{at} ({} {zone})", local.strftime("%Y-%m-%d %H:%M")),
        Err(_) => at.to_string(),
    }
}
