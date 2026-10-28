//! Rendering templates and command lines: references filled in from the run's inputs and
//! context, values written as text the same way the Python API writes literals.
//!
//! In a shell command every interpolated value is shell-quoted exactly like Python's
//! `shlex.quote`, unless its format spec is `raw` (see PROJECT.md, Templating: safe
//! substitution into shell commands).

use std::collections::BTreeMap;
use std::fmt::Write as _;

use jiff::civil::Time;
use jiff::fmt::temporal::Pieces;
use jiff::tz::TimeZone;
use schedr_model::{CommandLine, DagCtxField, PathElem, Reference, Template, TemplatePart};
use serde_json::{Value, json};

#[cfg(test)]
mod tests;

/// The program that runs shell commands.
pub const SHELL: &str = "/bin/sh";

/// The format spec that inserts a value into a shell command unquoted.
pub const RAW: &str = "raw";

/// Why a template can't be rendered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RenderError {
    /// The task's inputs don't include an upstream output it references.
    #[error("the output of `{0}` isn't among the task's inputs")]
    MissingInput(String),
    /// A path doesn't exist in the upstream output.
    #[error("`{0}` doesn't exist in the upstream output")]
    MissingPath(String),
    /// The referenced stream was truncated; partial data is never used silently.
    #[error("`{0}` was truncated (over the output size limit); read the whole ProcessResult")]
    Truncated(String),
    /// A value that can't be written as text (None, a list, an object).
    #[error("`{reference}` is {kind}, which can't be rendered into text")]
    NotRenderable {
        /// The reference.
        reference: String,
        /// What the value is.
        kind: &'static str,
    },
    /// A format spec that doesn't fit the value.
    #[error("`{reference}`: the format spec `:{spec}` doesn't apply: {reason}")]
    BadFormat {
        /// The reference.
        reference: String,
        /// The spec.
        spec: String,
        /// Why.
        reason: String,
    },
}

/// What references are filled in from.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RenderContext {
    /// The run's context fields, as stored JSON (`planned_for` / `since` tagged).
    pub dag_ctx: BTreeMap<DagCtxField, Value>,
    /// The upstream outputs the task uses, by task ID, as stored JSON.
    pub inputs: BTreeMap<String, Value>,
    /// The outcomes of the upstream tasks it uses, by task ID, as JSON strings; `null` for one
    /// that was skipped (each of its edges counts as not taken). A task missing here is
    /// neither: its reference fails as a missing input.
    pub outcomes: BTreeMap<String, Value>,
}

/// What a reference stands for when its edge wasn't taken (the data rule: `None`).
static NOT_TAKEN: Value = Value::Null;

impl RenderContext {
    /// A context with the run's fields; `planned_for` and `since` are RFC 3339 instants.
    #[must_use]
    pub fn new(dag_id: &str, run_id: &str, run_kind: &str, planned_for: &str, since: &str) -> Self {
        Self {
            dag_ctx: BTreeMap::from([
                (DagCtxField::DagId, json!(dag_id)),
                (DagCtxField::RunId, json!(run_id)),
                (DagCtxField::RunKind, json!(run_kind)),
                (DagCtxField::PlannedFor, json!({"$datetime": planned_for})),
                (DagCtxField::Since, json!({"$datetime": since})),
            ]),
            inputs: BTreeMap::new(),
            outcomes: BTreeMap::new(),
        }
    }
}

/// A reference as written in the DAG file, for messages (`extract.rows[0]`).
#[must_use]
pub fn reference_label(reference: &Reference) -> String {
    match reference {
        Reference::DagCtx {
            field: DagCtxField::DagId,
        } => "dag_ctx.dag_id".to_owned(),
        Reference::DagCtx { field } => {
            let name = serde_json::to_value(field).unwrap_or_default();
            let name = name.as_str().unwrap_or_default().trim_start_matches("run_");
            format!("dag_ctx.run.{name}")
        }
        Reference::Outcome { task } => format!("{task}.outcome"),
        Reference::Output { task, path, .. } => {
            let mut label = task.to_string();
            for step in path {
                match step {
                    PathElem::Field(name) => {
                        let _ = write!(label, ".{name}");
                    }
                    PathElem::Index(i) => {
                        let _ = write!(label, "[{i}]");
                    }
                }
            }
            label
        }
    }
}

/// The stored value a reference stands for.
///
/// # Errors
///
/// Returns a [`RenderError`] for a missing input or path, or a truncated stream.
pub fn resolve<'a>(
    reference: &Reference,
    ctx: &'a RenderContext,
) -> Result<&'a Value, RenderError> {
    match reference {
        Reference::DagCtx { field } => ctx
            .dag_ctx
            .get(field)
            .ok_or_else(|| RenderError::MissingInput(reference_label(reference))),
        Reference::Outcome { task } => match ctx.outcomes.get(task.as_str()) {
            Some(Value::Null) => Ok(&NOT_TAKEN),
            Some(outcome) => Ok(outcome),
            None => Err(RenderError::MissingInput(reference_label(reference))),
        },
        Reference::Output { task, path, on } => {
            match ctx.outcomes.get(task.as_str()) {
                // Skipped: every edge from it counts as not taken.
                Some(Value::Null) => return Ok(&NOT_TAKEN),
                Some(Value::String(outcome)) if !on.is_empty() && !on.contains(outcome) => {
                    return Ok(&NOT_TAKEN);
                }
                _ => {}
            }
            let mut value = ctx
                .inputs
                .get(task.as_str())
                .ok_or_else(|| RenderError::MissingInput(task.to_string()))?;
            for step in path {
                let missing = || RenderError::MissingPath(reference_label(reference));
                value = match step {
                    PathElem::Field(name) => {
                        let object = value.as_object().ok_or_else(missing)?;
                        if object.get(&format!("{name}_truncated")) == Some(&Value::Bool(true)) {
                            return Err(RenderError::Truncated(reference_label(reference)));
                        }
                        object.get(name).ok_or_else(missing)?
                    }
                    PathElem::Index(i) => {
                        let items = value
                            .get("$tuple")
                            .unwrap_or(value)
                            .as_array()
                            .ok_or_else(missing)?;
                        usize::try_from(*i)
                            .ok()
                            .and_then(|i| items.get(i))
                            .ok_or_else(missing)?
                    }
                };
            }
            Ok(value)
        }
    }
}

/// A float written like Python's `str()` (`1.0`, `1.5`, `1e+20`, `1e-05`).
#[must_use]
pub fn python_float(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 {
            "inf".to_owned()
        } else {
            "-inf".to_owned()
        };
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    if (-4..16).contains(&exponent) {
        let text = format!("{value}");
        if text.contains('.') {
            text
        } else {
            format!("{text}.0")
        }
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exponent.abs())
    }
}

fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "None",
        Value::Array(_) => "a list",
        Value::Object(_) => "an object",
        _ => "a value",
    }
}

/// A date-time or date from its stored text, in its own offset (midnight for a date).
fn zoned(text: &str, is_date: bool) -> Option<jiff::Zoned> {
    let pieces = Pieces::parse(text).ok()?;
    let offset = if is_date {
        jiff::tz::Offset::UTC
    } else {
        pieces.offset()?.to_numeric_offset()
    };
    let datetime = pieces
        .date()
        .to_datetime(pieces.time().unwrap_or(Time::midnight()));
    TimeZone::fixed(offset).to_zoned(datetime).ok()
}

/// A stored value as text.
///
/// # Errors
///
/// Returns [`RenderError::NotRenderable`] for None, lists and objects, or
/// [`RenderError::BadFormat`] for a spec that doesn't fit (strftime only on dates and times).
pub fn to_text(value: &Value, spec: Option<&str>, label: &str) -> Result<String, RenderError> {
    let spec = spec.filter(|s| !s.is_empty() && *s != RAW);
    let bad = |reason: &str| RenderError::BadFormat {
        reference: label.to_owned(),
        spec: spec.unwrap_or_default().to_owned(),
        reason: reason.to_owned(),
    };
    if let Some(object) = value.as_object()
        && object.len() == 1
    {
        if let Some(Value::String(text)) = object.get("$datetime").or_else(|| object.get("$date")) {
            let is_date = object.contains_key("$date");
            return match spec {
                None => Ok(text.clone()),
                Some(pattern) => {
                    let moment =
                        zoned(text, is_date).ok_or_else(|| bad("the stored date is invalid"))?;
                    jiff::fmt::strtime::format(pattern, &moment).map_err(|e| bad(&e.to_string()))
                }
            };
        }
        if let Some(micros) = object.get("$timedelta").and_then(Value::as_i64) {
            if spec.is_some() {
                return Err(bad("durations are written as seconds"));
            }
            let (seconds, rest) = (micros / 1_000_000, micros % 1_000_000);
            return Ok(if rest == 0 {
                seconds.to_string()
            } else {
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "a duration's seconds as a float"
                )]
                python_float(micros as f64 / 1_000_000.0)
            });
        }
    }
    if spec.is_some() {
        return Err(bad("only dates and times take a format spec"));
    }
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Bool(true) => Ok("True".to_owned()),
        Value::Bool(false) => Ok("False".to_owned()),
        Value::Number(n) if n.is_f64() => Ok(python_float(n.as_f64().unwrap_or_default())),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(RenderError::NotRenderable {
            reference: label.to_owned(),
            kind: kind_of(value),
        }),
    }
}

/// Quotes a string for a POSIX shell, exactly like Python's `shlex.quote`.
#[must_use]
pub fn shell_quote(text: &str) -> String {
    if text.is_empty() {
        return "''".to_owned();
    }
    let safe = text
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "_@%+=:,./-".contains(c));
    if safe {
        text.to_owned()
    } else {
        format!("'{}'", text.replace('\'', r#"'"'"'"#))
    }
}

/// Renders a template; in a shell command, references are quoted unless `raw`.
///
/// # Errors
///
/// Returns a [`RenderError`] if a reference can't be resolved or written as text.
pub fn render(
    template: &Template,
    ctx: &RenderContext,
    shell: bool,
) -> Result<String, RenderError> {
    let mut out = String::new();
    for part in &template.0 {
        match part {
            TemplatePart::Text(text) => out.push_str(text),
            TemplatePart::Ref(r) => {
                let label = reference_label(&r.reference);
                let text = to_text(resolve(&r.reference, ctx)?, r.format.as_deref(), &label)?;
                if shell && r.format.as_deref() != Some(RAW) {
                    out.push_str(&shell_quote(&text));
                } else {
                    out.push_str(&text);
                }
            }
        }
    }
    Ok(out)
}

/// The program and arguments of a command line.
///
/// A shell command runs as `/bin/sh -c <rendered>`; an argument list renders each element
/// unquoted, the first being the program.
///
/// # Errors
///
/// Returns a [`RenderError`] if a template can't be rendered.
pub fn command_line(
    command: &CommandLine,
    ctx: &RenderContext,
) -> Result<(String, Vec<String>), RenderError> {
    match command {
        CommandLine::Shell(template) => Ok((
            SHELL.to_owned(),
            vec!["-c".to_owned(), render(template, ctx, true)?],
        )),
        CommandLine::Argv(templates) => {
            let mut args = templates
                .iter()
                .map(|t| render(t, ctx, false))
                .collect::<Result<Vec<_>, _>>()?;
            let program = if args.is_empty() {
                String::new()
            } else {
                args.remove(0)
            };
            Ok((program, args))
        }
    }
}
