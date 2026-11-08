//! A run's parameters (decision 138): from the JSON a trigger gives to the form they are
//! stored and sent in, which is how outputs are stored (dates and tuples tagged), and back to
//! JSON for showing them.

use serde_json::{Map, Value, json};

use crate::TypeDesc;

#[cfg(test)]
mod tests;

/// At most this many bytes of a run's parameters as JSON, defaults included (decision 135).
pub const MAX_PARAMS_BYTES: usize = 64 * 1024;

/// The keys of tagged values, as the Python codec writes them.
const TAGS: [&str; 5] = ["$datetime", "$date", "$timedelta", "$tuple", "$dict"];

/// Microseconds in a day.
const DAY_MICROS: i128 = 86_400_000_000;

/// Values given as JSON (`{"day": "2026-10-01"}`), already checked against the schema, in the
/// form they are stored in, by the declared type (`{"day": {"$date": "2026-10-01"}}`).
///
/// # Errors
///
/// Where and why a value doesn't fit its type: a date that isn't one, a duration in months, a
/// value of a type outputs can't hold, text containing NUL. The reason never quotes the value.
pub fn to_stored(value: &Value, ty: &TypeDesc) -> Result<Value, Misfit> {
    checked(value, ty, Form::Given)
}

/// A value reported from outside Python (`schedr task-ctx resource set`), in the form it is
/// stored in: as [`to_stored`], and a dataclass gives every field, since nothing fills in its
/// defaults before a command reads the value (`t"{cluster.value.region}"`).
///
/// # Errors
///
/// As [`to_stored`], and a dataclass field left out.
pub fn to_reported(value: &Value, ty: &TypeDesc) -> Result<Value, Misfit> {
    checked(value, ty, Form::Whole)
}

/// Whether a dataclass may leave out its fields that have defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Form {
    /// Yes: Python fills them in where it reads the value.
    Given,
    /// No: the value is read as stored.
    Whole,
}

fn checked(value: &Value, ty: &TypeDesc, form: Form) -> Result<Value, Misfit> {
    let result = match nul_at(value) {
        Some(at) => Err((at, "text containing NUL (\\0) can't be stored".to_owned())),
        None => stored(value, ty, form),
    };
    result.map_err(|(path, why)| Misfit { path, why })
}

/// A value that doesn't fit its type. `why` doesn't quote the value; `path` names where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Misfit {
    /// Where in the parameters, outermost first (`config`, `"limits"`, `0`); empty for the
    /// whole.
    pub path: Vec<Step>,
    /// Why (`not an ISO 8601 duration`).
    pub why: String,
}

/// A step into a value: a field or key, or a list's index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// A field of an object, or a key of a dict.
    Key(String),
    /// An element of a list or tuple.
    Index(usize),
}

impl Misfit {
    /// The field names and keys along the path, as given (`api.key` stays one).
    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.path.iter().filter_map(|step| match step {
            Step::Key(key) => Some(key.as_str()),
            Step::Index(_) => None,
        })
    }

    /// The path as written (`config.limits[0]`); empty for the whole.
    #[must_use]
    pub fn at(&self) -> String {
        let mut at = String::new();
        for step in &self.path {
            match step {
                Step::Key(key) => {
                    if !at.is_empty() {
                        at.push('.');
                    }
                    at.push_str(key);
                }
                Step::Index(i) => {
                    at.push('[');
                    at.push_str(&i.to_string());
                    at.push(']');
                }
            }
        }
        at
    }
}

impl std::fmt::Display for Misfit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.path.is_empty() {
            f.write_str(&self.why)
        } else {
            write!(f, "`{}`: {}", self.at(), self.why)
        }
    }
}

impl std::error::Error for Misfit {}

/// A problem, with where in the value it is.
type Problem = (Vec<Step>, String);

fn misfit(why: impl Into<String>) -> Problem {
    (Vec::new(), why.into())
}

/// Where a text or a key containing NUL is (no store keeps one: Postgres refuses it in JSON,
/// the database layer replaces it (decision 114), and the runner refuses to decode it).
#[must_use]
pub fn nul_at(value: &Value) -> Option<Vec<Step>> {
    match value {
        Value::String(text) if text.contains('\0') => Some(Vec::new()),
        Value::Array(items) => items.iter().enumerate().find_map(|(i, v)| {
            nul_at(v).map(|mut path| {
                path.insert(0, Step::Index(i));
                path
            })
        }),
        Value::Object(object) => object.iter().find_map(|(k, v)| {
            if k.contains('\0') {
                // The key itself can't be named: it is the text refused.
                return Some(Vec::new());
            }
            nul_at(v).map(|mut path| {
                path.insert(0, Step::Key(k.clone()));
                path
            })
        }),
        _ => None,
    }
}

fn stored(value: &Value, ty: &TypeDesc, form: Form) -> Result<Value, Problem> {
    match ty {
        TypeDesc::Any | TypeDesc::Model { .. } => Ok(escape_tags(value)),
        TypeDesc::None if value.is_null() => Ok(Value::Null),
        TypeDesc::Bool if value.is_boolean() => Ok(value.clone()),
        TypeDesc::Int if value.is_i64() || value.is_u64() => Ok(value.clone()),
        // `3.0`: JSON Schema's `integer` takes a whole number written with a fraction.
        TypeDesc::Int if let Some(number) = whole(value) => Ok(Value::from(number)),
        TypeDesc::Float if value.is_number() => Ok(value.clone()),
        TypeDesc::Str if value.is_string() => Ok(value.clone()),
        TypeDesc::Datetime => text(value, "a date and time").and_then(|t| {
            t.parse::<jiff::Timestamp>()
                .map_err(|_| misfit("not a date and time with an offset (RFC 3339)"))
                .and_then(|_| python_year(t))
                .map(|()| json!({"$datetime": t}))
        }),
        TypeDesc::Date => text(value, "a date").and_then(|t| {
            t.parse::<jiff::civil::Date>()
                .map_err(|_| misfit("not a date (YYYY-MM-DD)"))
                .and_then(|_| python_year(t))
                .map(|()| json!({"$date": t}))
        }),
        TypeDesc::Timedelta => text(value, "a duration").and_then(|t| {
            duration_micros(t)
                .map(|micros| json!({"$timedelta": micros}))
                .map_err(misfit)
        }),
        TypeDesc::List { item } => {
            let items = array(value)?;
            Ok(Value::Array(elements(items, |_| item, form)?))
        }
        TypeDesc::Tuple { items: types } => {
            let items = array(value)?;
            if items.len() != types.len() {
                return Err(misfit(format!(
                    "expected {} elements, got {}",
                    types.len(),
                    items.len()
                )));
            }
            Ok(json!({"$tuple": elements(items, |i| &types[i], form)?}))
        }
        TypeDesc::Dict { value: item } => {
            let object = object(value)?;
            let mut out = Map::new();
            for (key, v) in object {
                out.insert(
                    key.clone(),
                    within(stored(v, item, form), Step::Key(key.clone()))?,
                );
            }
            Ok(escaped(out))
        }
        TypeDesc::Literal { values } if values.contains(value) => Ok(value.clone()),
        // A type that checks the value first: a model or `Any` takes anything, and would keep a
        // tuple or a date untagged.
        TypeDesc::Union { variants } => variants
            .iter()
            .filter(|v| !matches!(v, TypeDesc::Any | TypeDesc::Model { .. }))
            .chain(
                variants
                    .iter()
                    .filter(|v| matches!(v, TypeDesc::Any | TypeDesc::Model { .. })),
            )
            .find_map(|v| stored(value, v, form).ok())
            .ok_or_else(|| misfit(format!("{} fits none of its types", kind_of(value)))),
        TypeDesc::TypedDict { fields, .. } | TypeDesc::Dataclass { fields, .. } => {
            let object = object(value)?;
            let mut out = Map::new();
            for (key, v) in object {
                let field = fields
                    .iter()
                    .find(|f| &f.name == key)
                    .ok_or_else(|| misfit(format!("unknown field `{key}`")))?;
                out.insert(
                    key.clone(),
                    within(stored(v, &field.ty, form), Step::Key(key.clone()))?,
                );
            }
            let whole = form == Form::Whole && matches!(ty, TypeDesc::Dataclass { .. });
            if let Some(missing) = fields
                .iter()
                .find(|f| (f.required || whole) && !out.contains_key(&f.name))
            {
                return Err(misfit(format!("`{}` is required", missing.name)));
            }
            Ok(escaped(out))
        }
        TypeDesc::ProcessResult => Err(misfit("a command's result can't be a parameter")),
        TypeDesc::None
        | TypeDesc::Bool
        | TypeDesc::Int
        | TypeDesc::Float
        | TypeDesc::Str
        | TypeDesc::Literal { .. } => Err(misfit(format!(
            "expected {}, got {}",
            expected(ty),
            kind_of(value)
        ))),
    }
}

/// Text that starts with a year Python's `date` and `datetime` read: four digits, 0001 to
/// 9999 (not `+002026`, nor year 0).
fn python_year(text: &str) -> Result<(), Problem> {
    let year = text
        .get(..4)
        .filter(|y| y.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|y| y.parse::<u16>().ok());
    match year {
        Some(1..=9999) if text.as_bytes().get(4) == Some(&b'-') => Ok(()),
        _ => Err(misfit(
            "a year Python can't read: four digits, 0001 to 9999",
        )),
    }
}

/// A number with no fraction (`3.0`) as an integer, if it fits one.
fn whole(value: &Value) -> Option<i64> {
    // Up to 2^53, where every whole number is exact.
    const EXACT: f64 = 9_007_199_254_740_992.0;
    let number = value.as_f64().filter(|n| n.fract() == 0.0)?;
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a whole number, checked in range"
    )]
    (-EXACT..=EXACT).contains(&number).then_some(number as i64)
}

fn within(result: Result<Value, Problem>, step: Step) -> Result<Value, Problem> {
    result.map_err(|(mut path, why)| {
        path.insert(0, step);
        (path, why)
    })
}

fn elements<'t>(
    items: &[Value],
    ty: impl Fn(usize) -> &'t TypeDesc,
    form: Form,
) -> Result<Vec<Value>, Problem> {
    items
        .iter()
        .enumerate()
        .map(|(i, v)| within(stored(v, ty(i), form), Step::Index(i)))
        .collect()
}

fn text<'v>(value: &'v Value, what: &str) -> Result<&'v str, Problem> {
    value
        .as_str()
        .ok_or_else(|| misfit(format!("expected {what} as text, got {}", kind_of(value))))
}

fn array(value: &Value) -> Result<&Vec<Value>, Problem> {
    value
        .as_array()
        .ok_or_else(|| misfit(format!("expected a list, got {}", kind_of(value))))
}

fn object(value: &Value) -> Result<&Map<String, Value>, Problem> {
    value
        .as_object()
        .ok_or_else(|| misfit(format!("expected an object, got {}", kind_of(value))))
}

/// An ISO 8601 duration (`P1DT2H`, `PT0.5S`, `-PT1M`, `P1W`) in whole microseconds, a day
/// being 24 hours and a week 7 days (decision 139).
///
/// # Errors
///
/// Why it isn't one, without quoting it: not ISO 8601, in years or months (they aren't fixed
/// lengths), or too long.
pub fn duration_micros(text: &str) -> Result<i64, &'static str> {
    let span = jiff::fmt::temporal::SpanParser::new()
        .parse_span(text)
        .map_err(|_| "not an ISO 8601 duration (e.g. PT1H30M, P1DT0.5S)")?;
    if span.get_years() != 0 || span.get_months() != 0 {
        return Err(
            "a duration in years or months, which vary: give it in weeks, days, hours, minutes \
             and seconds",
        );
    }
    let micros = i128::from(span.get_weeks()) * 7 * DAY_MICROS
        + i128::from(span.get_days()) * DAY_MICROS
        + i128::from(span.get_hours()) * 3_600_000_000
        + i128::from(span.get_minutes()) * 60_000_000
        + i128::from(span.get_seconds()) * 1_000_000
        + i128::from(span.get_milliseconds()) * 1000
        + i128::from(span.get_microseconds())
        + i128::from(span.get_nanoseconds()) / 1000;
    i64::try_from(micros).map_err(|_| "a duration too long to store")
}

/// A dict stored escaped when its only key would read as a tag (B66).
#[must_use]
pub fn escaped(object: Map<String, Value>) -> Value {
    if object.len() == 1 && object.keys().all(|k| TAGS.contains(&k.as_str())) {
        json!({ "$dict": object })
    } else {
        Value::Object(object)
    }
}

/// Every dict shaped like a tag escaped, at any depth (a model's or `Any`'s data).
#[must_use]
pub fn escape_tags(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(escape_tags).collect()),
        Value::Object(object) => escaped(
            object
                .iter()
                .map(|(k, v)| (k.clone(), escape_tags(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(n) if n.is_f64() => "a number",
        Value::Number(_) => "an integer",
        Value::String(_) => "text",
        Value::Array(_) => "a list",
        Value::Object(_) => "an object",
    }
}

const fn expected(ty: &TypeDesc) -> &'static str {
    match ty {
        TypeDesc::None => "null",
        TypeDesc::Bool => "a boolean",
        TypeDesc::Int => "an integer",
        TypeDesc::Float => "a number",
        TypeDesc::Str => "text",
        TypeDesc::Literal { .. } => "one of its values",
        _ => "another type",
    }
}

/// Stored parameters as JSON, for showing them: dates and times as ISO 8601 text, a duration
/// as an ISO 8601 duration, a tuple as a list, an escaped dict unwrapped (what
/// `--params` takes).
#[must_use]
pub fn shown(stored: &Value) -> Value {
    match stored {
        Value::Array(items) => Value::Array(items.iter().map(shown).collect()),
        Value::Object(object) if object.len() == 1 => {
            let (tag, inner) = object.iter().next().unwrap_or_else(|| unreachable!());
            match (tag.as_str(), inner) {
                ("$datetime" | "$date", Value::String(_)) => inner.clone(),
                ("$tuple", Value::Array(_)) | ("$dict", Value::Object(_)) => shown_fields(inner),
                ("$timedelta", Value::Number(n)) if n.is_i64() => {
                    Value::String(iso_duration(n.as_i64().unwrap_or_default()))
                }
                _ => shown_fields(stored),
            }
        }
        _ => shown_fields(stored),
    }
}

fn shown_fields(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(shown).collect()),
        Value::Object(object) => {
            Value::Object(object.iter().map(|(k, v)| (k.clone(), shown(v))).collect())
        }
        other => other.clone(),
    }
}

/// Microseconds as the Python layer writes a duration for Pydantic (`P1DT3600.000000S`).
fn iso_duration(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let all = i128::from(micros).abs();
    let (days, rest) = (all / DAY_MICROS, all % DAY_MICROS);
    format!(
        "{sign}P{days}DT{}.{:06}S",
        rest / 1_000_000,
        rest % 1_000_000
    )
}
