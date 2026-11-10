//! Notifications in DAG JSON (decisions 408, 409, 417-423): which channels a DAG tells of which
//! events, the channels a DAG defines itself, and message bodies as data.
//!
//! A body is JSON whose strings may be templates. A DAG writes them as t-strings over
//! `schedr.notify` placeholders; the server configuration as `"{name}"` strings. Both arrive
//! here as the same [`Body`]: text parts and references, which the master fills value by value.
//! Nothing in a body is code, and filling one never runs anything.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// An event a channel can be told of (decision 408; `Notify` in Python, decision 409).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NotifyEvent {
    /// A run ended `failed` (a cancel with a failure included).
    RunFailed,
    /// A task failed after its last retry.
    TaskFailed,
    /// An attempt failed and will be retried.
    TaskRetry,
    /// A run was cancelled, with no failure.
    RunCancelled,
    /// A run ended `success`.
    RunSucceeded,
    /// A run succeeded after the DAG's previous run failed.
    RunRecovered,
    /// The DAG's trigger kept failing (`[triggers] failing_after`), or its generator returned.
    TriggerFailing,
    /// A cleanup that ran after its run had ended failed (decision 309).
    LateCleanupFailed,
}

impl NotifyEvent {
    /// Every event, in the order above.
    pub const ALL: [Self; 8] = [
        Self::RunFailed,
        Self::TaskFailed,
        Self::TaskRetry,
        Self::RunCancelled,
        Self::RunSucceeded,
        Self::RunRecovered,
        Self::TriggerFailing,
        Self::LateCleanupFailed,
    ];

    /// Its name, as written in Python and in configuration (`RUN_FAILED`).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::RunFailed => "RUN_FAILED",
            Self::TaskFailed => "TASK_FAILED",
            Self::TaskRetry => "TASK_RETRY",
            Self::RunCancelled => "RUN_CANCELLED",
            Self::RunSucceeded => "RUN_SUCCEEDED",
            Self::RunRecovered => "RUN_RECOVERED",
            Self::TriggerFailing => "TRIGGER_FAILING",
            Self::LateCleanupFailed => "LATE_CLEANUP_FAILED",
        }
    }

    /// The event named `name`, if any.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|event| event.name() == name)
    }
}

impl fmt::Display for NotifyEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// One channel a DAG notifies (`notify=[...]` or `notify={channel: [...]}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NotifyEntry {
    /// The channel.
    pub channel: ChannelRef,
    /// The events it is told of; none: the server's default events
    /// (`[notifications] default_events`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events: Option<Vec<NotifyEvent>>,
}

/// A channel: one the server configuration defines (named by a string in Python), or one the
/// DAG defines itself (an object).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChannelRef {
    /// `[notifications.channels.<name>]`: a webhook or an email channel.
    Server {
        /// Its name in the configuration.
        name: String,
    },
    /// A `WebhookChannel` of the DAG's own (decision 425).
    Webhook(WebhookChannel),
}

impl ChannelRef {
    /// The channel's name, as `send` and `schedr task-ctx notify` name it.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Server { name } => name,
            Self::Webhook(webhook) => &webhook.name,
        }
    }
}

/// A webhook a DAG defines (decision 425): its URL must start with one of
/// `[notifications] dag_urls`, and it carries no secrets (its URL is in the DAG file).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WebhookChannel {
    /// Its name, unique among the DAG's channels.
    pub name: String,
    /// Where it posts.
    pub url: String,
    /// The JSON it posts; none: schedr's fixed JSON (decision 417).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Body>,
}

/// A message body (decision 417): JSON, with values filled in when a notification is queued.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Body {
    /// A JSON value as it is.
    Value(serde_json::Value),
    /// A placeholder's value, keeping its JSON type.
    Ref(Ref),
    /// A string of text parts and placeholders.
    Template(Vec<Part>),
    /// An object of bodies.
    Object(std::collections::BTreeMap<String, Body>),
    /// An array of bodies.
    Array(Vec<Body>),
}

impl Body {
    /// Every reference in it, in order.
    #[must_use]
    pub fn refs(&self) -> Vec<&Ref> {
        let mut out = Vec::new();
        self.collect_refs(&mut out);
        out
    }

    fn collect_refs<'a>(&'a self, out: &mut Vec<&'a Ref>) {
        match self {
            Self::Value(_) => {}
            Self::Ref(reference) => out.push(reference),
            Self::Template(parts) => out.extend(parts.iter().filter_map(|part| match part {
                Part::Ref(reference) => Some(reference),
                Part::Text(_) => None,
            })),
            Self::Object(map) => map.values().for_each(|body| body.collect_refs(out)),
            Self::Array(items) => items.iter().for_each(|body| body.collect_refs(out)),
        }
    }

    /// A body written in the server configuration: JSON whose strings may hold `{name}`
    /// placeholders (`{{` and `}}` for literal braces). A string that is exactly one
    /// placeholder is a [`Body::Ref`], keeping the value's JSON type.
    ///
    /// # Errors
    ///
    /// A string with an unknown placeholder, an unclosed `{`, or a lone `}`; the message names
    /// the string.
    pub fn from_config(value: &serde_json::Value) -> Result<Self, String> {
        Ok(match value {
            serde_json::Value::String(text) => {
                let parts = parse_template(text)?;
                match parts.as_slice() {
                    [Part::Ref(reference)] => Self::Ref(reference.clone()),
                    [] => Self::Value(serde_json::Value::String(String::new())),
                    [Part::Text(text)] => Self::Value(serde_json::Value::String(text.clone())),
                    _ => Self::Template(parts),
                }
            }
            serde_json::Value::Array(items) => Self::Array(
                items
                    .iter()
                    .map(Self::from_config)
                    .collect::<Result<_, _>>()?,
            ),
            serde_json::Value::Object(map) => Self::Object(
                map.iter()
                    .map(|(key, value)| Ok((key.clone(), Self::from_config(value)?)))
                    .collect::<Result<_, String>>()?,
            ),
            other => Self::Value(other.clone()),
        })
    }
}

/// A piece of a [`Body::Template`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Part {
    /// Text as it is.
    Text(String),
    /// A placeholder's value, as text (a string as it is, anything else as its JSON).
    Ref(Ref),
}

/// A placeholder, and a path into its value (`data['rows']`, decision 423).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Ref {
    /// The placeholder.
    pub name: Placeholder,
    /// Keys and indexes into its value, outermost first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path: Vec<PathKey>,
}

/// A step of a [`Ref`]'s path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum PathKey {
    /// An index into an array.
    Index(u64),
    /// A key of an object.
    Key(String),
}

impl fmt::Display for Ref {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name.name())?;
        for key in &self.path {
            match key {
                PathKey::Index(index) => write!(f, "[{index}]")?,
                PathKey::Key(key) => write!(f, "[{key:?}]")?,
            }
        }
        Ok(())
    }
}

/// The values a body can use (decision 417).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Placeholder {
    /// The event's name (`RUN_FAILED`; `MESSAGE` for a task's own `send`).
    Event,
    /// The DAG's ID.
    Dag,
    /// The run's ID.
    Run,
    /// The run's planned time (UTC, RFC 3339), or null.
    PlannedFor,
    /// The run's or the task's state.
    State,
    /// schedr's one-line summary; a task's own `send`: its `text`.
    Text,
    /// The link to the run in the UI (`[notifications] ui_url`), or null.
    Link,
    /// The failed tasks: a list of `{task, error}`.
    FailedTasks,
    /// The task's ID, for task events and `send`; null otherwise.
    Task,
    /// The attempt's number, for task events and `send`; null otherwise.
    Attempt,
    /// When the event happened (UTC, RFC 3339).
    Time,
    /// The DAG's time zone.
    Zone,
    /// A task's own `send`: its `data`; null in event notifications.
    Data,
}

impl Placeholder {
    /// Every placeholder.
    pub const ALL: [Self; 13] = [
        Self::Event,
        Self::Dag,
        Self::Run,
        Self::PlannedFor,
        Self::State,
        Self::Text,
        Self::Link,
        Self::FailedTasks,
        Self::Task,
        Self::Attempt,
        Self::Time,
        Self::Zone,
        Self::Data,
    ];

    /// Its name, as written in templates.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Dag => "dag",
            Self::Run => "run",
            Self::PlannedFor => "planned_for",
            Self::State => "state",
            Self::Text => "text",
            Self::Link => "link",
            Self::FailedTasks => "failed_tasks",
            Self::Task => "task",
            Self::Attempt => "attempt",
            Self::Time => "time",
            Self::Zone => "zone",
            Self::Data => "data",
        }
    }

    /// The placeholder named `name`, if any.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|placeholder| placeholder.name() == name)
    }
}

/// `"[ops] {dag} {data[rows]}"` into parts: `{name}` or `{name[key]...}` (a key of digits is an
/// index), `{{` and `}}` literal braces.
fn parse_template(text: &str) -> Result<Vec<Part>, String> {
    let mut parts = Vec::new();
    let mut literal = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                literal.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                literal.push('}');
            }
            '}' => return Err(format!("{text:?}: a lone `}}` (write `}}}}` for a brace)")),
            '{' => {
                let mut inner = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some(c) => inner.push(c),
                        None => return Err(format!("{text:?}: a `{{` that isn't closed")),
                    }
                }
                if !literal.is_empty() {
                    parts.push(Part::Text(std::mem::take(&mut literal)));
                }
                parts.push(Part::Ref(
                    parse_ref(&inner).map_err(|e| format!("{text:?}: {e}"))?,
                ));
            }
            c => literal.push(c),
        }
    }
    if !literal.is_empty() {
        parts.push(Part::Text(literal));
    }
    Ok(parts)
}

fn parse_ref(inner: &str) -> Result<Ref, String> {
    let (name, mut rest) = inner.find('[').map_or((inner, ""), |at| inner.split_at(at));
    let placeholder = Placeholder::parse(name.trim()).ok_or_else(|| {
        let known: Vec<&str> = Placeholder::ALL.iter().map(|p| p.name()).collect();
        format!(
            "unknown placeholder `{{{inner}}}` (known: {})",
            known.join(", ")
        )
    })?;
    let mut path = Vec::new();
    while !rest.is_empty() {
        let end = rest
            .strip_prefix('[')
            .and_then(|tail| tail.find(']'))
            .ok_or_else(|| format!("`{{{inner}}}`: a path is `[key]` after the name"))?;
        let key = &rest[1..=end];
        path.push(
            key.parse::<u64>()
                .map_or_else(|_| PathKey::Key(key.to_owned()), PathKey::Index),
        );
        rest = &rest[end + 2..];
    }
    Ok(Ref {
        name: placeholder,
        path,
    })
}
