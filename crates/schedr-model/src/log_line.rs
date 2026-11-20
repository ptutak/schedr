//! A task's structured log line (decision 514): one JSON object on a line of its stdout or
//! stderr, `{"schedr_log": 1, "level": ..., "message": ..., "fields": {...}, "time": ...}`,
//! written by schedr's helpers in any task kind; any other line is a plain one. Stored as
//! written, or as its readable form when `[worker] log_format = "plain"`; a download renders it
//! readable on asking.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// The key a structured line starts with, and its format's version.
pub const KEY: &str = "schedr_log";

/// The format's version.
pub const VERSION: u32 = 1;

/// A structured line's level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    /// Detail for debugging.
    Debug,
    /// What happens.
    Info,
    /// Something to look at.
    Warning,
    /// Something failed.
    Error,
    /// The task can't go on.
    Critical,
}

impl Level {
    /// Its name, as shown: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`.
    #[must_use]
    pub const fn shown(self) -> &'static str {
        match self {
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
            Self::Critical => "CRITICAL",
        }
    }

    /// Its name in a structured line.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Critical => "critical",
        }
    }

    /// The level a name names (as written, or Python's `warn`, `fatal`).
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "debug" => Self::Debug,
            "info" => Self::Info,
            "warning" | "warn" => Self::Warning,
            "error" => Self::Error,
            "critical" | "fatal" => Self::Critical,
            _ => return None,
        })
    }
}

/// One structured line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogLine {
    /// Always [`VERSION`].
    pub schedr_log: u32,
    /// Its level.
    pub level: Level,
    /// What it says.
    pub message: String,
    /// More about it: any JSON values by name (a logger's name, an exception's traceback).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, serde_json::Value>,
    /// When, as RFC 3339 text; none: the writer didn't say.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
}

impl LogLine {
    /// A line at `level` saying `message`.
    #[must_use]
    pub fn new(level: Level, message: impl Into<String>) -> Self {
        Self {
            schedr_log: VERSION,
            level,
            message: message.into(),
            fields: BTreeMap::new(),
            time: None,
        }
    }

    /// The structured line `line` (without its end of line) is; none for a plain one.
    #[must_use]
    pub fn parse(line: &str) -> Option<Self> {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        // Cheap first: a plain line is never parsed.
        if !trimmed.starts_with('{') || !trimmed.contains(KEY) {
            return None;
        }
        let parsed: Self = serde_json::from_str(trimmed).ok()?;
        (parsed.schedr_log == VERSION).then_some(parsed)
    }

    /// Its JSON line, without an end of line.
    #[must_use]
    pub fn json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Its readable form, without an end of line: its time (when given), its level, its
    /// message, then each field as `name=value` (text quoted when it has a space, a quote or
    /// an `=`; other values as JSON); a multi-line field (a traceback) after it, indented.
    #[must_use]
    pub fn plain(&self) -> String {
        use std::fmt::Write as _;
        let mut text = String::new();
        if let Some(time) = &self.time {
            text.push_str(time);
            text.push(' ');
        }
        let _ = write!(text, "{:<8} {}", self.level.shown(), self.message);
        let mut long = Vec::new();
        for (name, value) in &self.fields {
            let shown = match value {
                serde_json::Value::String(s) if s.contains('\n') => {
                    long.push((name, s));
                    continue;
                }
                serde_json::Value::String(s) if s.is_empty() || s.contains([' ', '"', '=']) => {
                    serde_json::to_string(s).unwrap_or_default()
                }
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let _ = write!(text, "  {name}={shown}");
        }
        for (name, value) in long {
            let _ = write!(text, "\n  {name}:");
            for line in value.trim_end().lines() {
                text.push_str("\n    ");
                text.push_str(line);
            }
        }
        text
    }
}

/// `text` (a log, or a part of one) with each structured line in its readable form; plain lines
/// and the end of each line as they were.
#[must_use]
pub fn rendered(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        match LogLine::parse(line) {
            Some(parsed) => {
                out.push_str(&parsed.plain());
                if line.ends_with('\n') {
                    out.push('\n');
                }
            }
            None => out.push_str(line),
        }
    }
    out
}
