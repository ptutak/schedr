//! The trigger dialog's run parameters (M4.5c, decisions 149-152): an input per field of the
//! DAG's params, as its API view describes them, pre-filled with the defaults; and the posted
//! form read back into the values a trigger sends, only those changed from the defaults (the
//! master fills in the rest and checks them all).
//!
//! The form's names: `p:<field>` the value, `none:<field>` the "none" box of a field that may
//! be `null`, and `params` marking a form that has parameters (an unchecked box sends nothing).
//! A message never quotes a value given (decision 134).

use askama::Template;
use schedr_api::views::{ParamFieldView, ParamKind};
use serde_json::{Map, Value};

/// The parameters' part of the trigger dialog.
#[derive(Template)]
#[template(path = "trigger_params.html")]
pub struct TriggerParams {
    /// One per field.
    pub inputs: Vec<ParamInput>,
    /// The fields' [`fingerprint`], posted as `params`.
    pub fingerprint: String,
}

impl TriggerParams {
    /// The dialog's part for `fields`.
    #[must_use]
    pub fn of(fields: &[ParamFieldView]) -> Self {
        Self {
            inputs: fields.iter().map(ParamInput::of).collect(),
            fingerprint: fingerprint(fields),
        }
    }
}

/// What the form's inputs were built from, as a short text: the fields' names, kinds, `null`,
/// secrecy, choices and defaults. A POST from a page built before the DAG's params changed is
/// refused, rather than read against fields it didn't show (a field dropped, a new box read as
/// unchecked) or sending a default since replaced as if it were chosen (decision 159). A
/// secret's default is never on the page nor in the view, so not here either: its empty box
/// sends nothing, and the master fills in the current one.
#[must_use]
pub fn fingerprint(fields: &[ParamFieldView]) -> String {
    let shape: Vec<Value> = fields
        .iter()
        .map(|f| {
            serde_json::json!([
                f.name,
                f.kind,
                f.nullable,
                f.secret,
                f.required,
                f.choices,
                f.default.is_some(),
                f.default,
            ])
        })
        .collect();
    // FNV-1a, 64 bits: stable across builds (the page and the POST may meet different UIs).
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in Value::Array(shape).to_string().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// One field's input, ready for the template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamInput {
    /// The field's name, shown.
    pub name: String,
    /// What the input is: `checkbox`, `select`, `textarea` or `input`.
    pub control: &'static str,
    /// An `input`'s type: `text`, `number`, `date` or `password`.
    pub input_type: &'static str,
    /// A number input's `step` (`1`, `any`); empty for others.
    pub step: &'static str,
    /// Its starting value.
    pub value: String,
    /// Shown while it is empty.
    pub placeholder: String,
    /// Whether a check box starts checked.
    pub checked: bool,
    /// A `select`'s options: the value as JSON, its label, whether it is the default.
    pub options: Vec<(String, String, bool)>,
    /// Whether the browser asks for it before sending.
    pub required: bool,
    /// Its "none" box, if it may be `null`: whether the box starts checked (the default is
    /// `null`).
    pub none: Option<bool>,
}

impl ParamInput {
    /// The input for `field`.
    #[must_use]
    pub fn of(field: &ParamFieldView) -> Self {
        let default = field.default.as_ref().filter(|v| !v.is_null());
        let mut input = Self {
            name: field.name.clone(),
            control: "input",
            input_type: "text",
            step: "",
            value: default.map(as_text).unwrap_or_default(),
            placeholder: if field.required {
                "required".to_owned()
            } else {
                String::new()
            },
            checked: false,
            options: Vec::new(),
            // A box left empty is the default; a required one the browser asks for.
            // (Not with a "none" box: checking it is an answer too.)
            required: field.required && field.kind != ParamKind::Bool && !field.nullable,
            none: field
                .nullable
                .then(|| field.default.as_ref().is_some_and(Value::is_null)),
        };
        if field.secret {
            // Decision 150: its default never reaches the page.
            input.input_type = "password";
            input.value = String::new();
            if !field.required {
                "leave empty for the default".clone_into(&mut input.placeholder);
            }
            return input;
        }
        match field.kind {
            ParamKind::Bool => {
                input.control = "checkbox";
                input.checked = default == Some(&Value::Bool(true));
            }
            ParamKind::Int | ParamKind::Float => {
                input.input_type = "number";
                input.step = if field.kind == ParamKind::Int {
                    "1"
                } else {
                    "any"
                };
            }
            ParamKind::Date => input.input_type = "date",
            ParamKind::Datetime => {
                if input.placeholder.is_empty() {
                    "2026-10-15T03:00:00+02:00".clone_into(&mut input.placeholder);
                }
            }
            ParamKind::Duration => {
                if input.placeholder.is_empty() {
                    "PT1H30M".clone_into(&mut input.placeholder);
                }
            }
            ParamKind::Choice => {
                input.control = "select";
                // Mixed kinds (`Literal["1", 1]`) are labelled as JSON, so they differ.
                let texts = field.choices.iter().all(Value::is_string);
                input.options = field
                    .choices
                    .iter()
                    .map(|c| {
                        let label = if texts { as_text(c) } else { c.to_string() };
                        (c.to_string(), label, Some(c) == default)
                    })
                    .collect();
                if default.is_none() {
                    input
                        .options
                        .insert(0, (String::new(), "choose".to_owned(), true));
                }
            }
            ParamKind::Json => {
                input.control = "textarea";
                input.value = default
                    .map(|v| serde_json::to_string_pretty(v).unwrap_or_default())
                    .unwrap_or_default();
                if input.placeholder.is_empty() {
                    "JSON".clone_into(&mut input.placeholder);
                }
            }
            ParamKind::Text => {}
        }
        input
    }
}

/// A value as an input shows it: a string as itself, anything else as JSON.
fn as_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// The values the posted `form` gives for `fields`, those differing from their defaults; none
/// when the form has no parameters. A field left empty (or a secret one) is left out, so the
/// master uses its default, or names it missing.
///
/// # Errors
///
/// What is wrong with a field, naming it but not quoting what was entered.
pub fn values(
    fields: &[ParamFieldView],
    form: &[(String, String)],
) -> Result<Option<Map<String, Value>>, String> {
    let Some((_, posted)) = form.iter().find(|(k, _)| k == "params") else {
        return Ok(None);
    };
    if *posted != fingerprint(fields) {
        return Err(
            "the DAG's parameters changed since this page was loaded: reload it and \
                    trigger again"
                .to_owned(),
        );
    }
    let get = |key: String| {
        form.iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.as_str())
    };
    let mut values = Map::new();
    for field in fields {
        let name = &field.name;
        let raw = get(format!("p:{name}"));
        let value = if field.nullable && get(format!("none:{name}")).is_some() {
            Some(Value::Null)
        } else if field.secret {
            raw.filter(|r| !r.is_empty()).map(|r| secret(field.kind, r))
        } else {
            entered(field, raw)?
        };
        if let Some(value) = value
            && field.default.as_ref() != Some(&value)
        {
            values.insert(name.clone(), value);
        }
    }
    Ok(Some(values))
}

/// A non-secret field's value from its input; none when it is left empty.
fn entered(field: &ParamFieldView, raw: Option<&str>) -> Result<Option<Value>, String> {
    let name = &field.name;
    if field.kind == ParamKind::Bool {
        return Ok(Some(Value::Bool(raw.is_some())));
    }
    let Some(raw) = raw else { return Ok(None) };
    let trimmed = raw.trim();
    if field.kind == ParamKind::Text {
        // An emptied text is the empty string, unless there is no default to differ from.
        return Ok((!raw.is_empty() || field.default.is_some()).then(|| Value::from(raw)));
    }
    if trimmed.is_empty() {
        return Ok(None);
    }
    let value = match field.kind {
        ParamKind::Int => trimmed
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| trimmed.parse::<u64>().map(Value::from))
            .map_err(|_| format!("`{name}` isn't a whole number"))?,
        ParamKind::Float => trimmed
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| format!("`{name}` isn't a number"))?,
        ParamKind::Choice => serde_json::from_str::<Value>(trimmed)
            .ok()
            .filter(|v| field.choices.contains(v))
            .ok_or_else(|| format!("`{name}` isn't one of its choices"))?,
        ParamKind::Json => serde_json::from_str::<Value>(trimmed).map_err(|e| {
            format!(
                "`{name}` isn't JSON (line {}, column {})",
                e.line(),
                e.column()
            )
        })?,
        _ => Value::from(trimmed),
    };
    Ok(Some(value))
}

/// A secret field's typed text: JSON when its kind isn't text and it reads as JSON (a secret
/// `Literal`'s `123` a number, `"123"` a string), else the text (the master checks it against
/// the field's type).
fn secret(kind: ParamKind, raw: &str) -> Value {
    let read = match kind {
        ParamKind::Int
        | ParamKind::Float
        | ParamKind::Bool
        | ParamKind::Json
        | ParamKind::Choice => serde_json::from_str::<Value>(raw.trim()).ok(),
        _ => None,
    };
    read.unwrap_or_else(|| Value::from(raw))
}

#[cfg(test)]
mod tests;
