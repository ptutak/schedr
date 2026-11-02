//! A DAG's run parameters as the UI's trigger form shows them (decisions 149-152): the
//! webserver reads the declaration (a dataclass's type, a Pydantic model's JSON Schema) so the
//! UI never interprets JSON Schema, and withholds the defaults it mustn't show.

use schedr_config::masked::{mask_data, names_secret_in_words};
use schedr_model::TypeDesc;
use schedr_model::dag::Params;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[cfg(test)]
mod tests;

/// One field of a DAG's run parameters, as a form shows it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ParamFieldView {
    /// The field's name, as a trigger's values name it.
    pub name: String,
    /// How it is entered.
    pub kind: ParamKind,
    /// Whether a trigger must give it (it has no default).
    pub required: bool,
    /// Whether it may be `null` (`T | None`): the form has a "none" box beside it.
    pub nullable: bool,
    /// Whether its default and choices are withheld (decision 150): its name has a word naming
    /// a secret, or its default or a choice holds one. The form gives it an empty password
    /// input; left empty, the master uses the default.
    pub secret: bool,
    /// Its default as JSON, the form's starting value (`null` is a default); absent when it
    /// has none or it is withheld.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    #[schema(value_type = Option<Object>)]
    pub default: Option<Value>,
    /// A `choice`'s values, as JSON (strings, numbers, booleans; `null` is `nullable`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(value_type = Vec<Object>)]
    pub choices: Vec<Value>,
}

/// A field that is present, `null` included (serde reads a `null` `Option` as absent).
fn present<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

/// How a parameter is entered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParamKind {
    /// `bool`: a check box.
    Bool,
    /// `int`: a whole number.
    Int,
    /// `float`: a number.
    Float,
    /// `str`: text.
    Text,
    /// `date`: `2026-10-15`.
    Date,
    /// `datetime`: RFC 3339 text with its offset.
    Datetime,
    /// `timedelta`: an ISO 8601 duration (`PT1H30M`).
    Duration,
    /// A `Literal` (or an `enum` in a model's schema): one of `choices`.
    Choice,
    /// Anything else (a nested dataclass or model, a list, a tuple, a dict, a union): JSON text
    /// (decision 152), as `--params` takes it.
    Json,
}

impl ParamFieldView {
    /// The fields of `params`: a dataclass's in declaration order, a model's by name (its
    /// schema's properties, as stored, keep no order).
    #[must_use]
    pub fn of(params: &Params) -> Vec<Self> {
        let required: Vec<&str> = root_object(&params.schema)
            .get("required")
            .and_then(Value::as_array)
            .map(|names| names.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let shapes: Vec<(String, Shape)> = match &params.ty {
            TypeDesc::Dataclass { fields, .. } => fields
                .iter()
                .map(|f| (f.name.clone(), Shape::of_type(&f.ty)))
                .collect(),
            _ => root_object(&params.schema)
                .get("properties")
                .and_then(Value::as_object)
                .map(|properties| {
                    properties
                        .iter()
                        .map(|(name, schema)| {
                            (name.clone(), Shape::of_schema(schema, &params.schema, 0))
                        })
                        .collect()
                })
                .unwrap_or_default(),
        };
        shapes
            .into_iter()
            .map(|(name, shape)| {
                let default = params.defaults.get(&name).cloned();
                let secret = names_secret_in_words(&name)
                    || default
                        .iter()
                        .chain(&shape.choices)
                        .any(|v| holds_secret(&name, v));
                Self {
                    required: default.is_none() && required.contains(&name.as_str()),
                    kind: shape.kind,
                    nullable: shape.nullable,
                    // A `Literal`'s values are copies of values too.
                    choices: if secret { Vec::new() } else { shape.choices },
                    default: if secret { None } else { default },
                    secret,
                    name,
                }
            })
            .collect()
    }
}

/// The schema of a model's object: the root, or what a root `$ref` names (Pydantic writes a
/// recursive model as `{"$ref": "#/$defs/P", "$defs": {...}}`).
fn root_object(root: &Value) -> &Value {
    root.get("$ref")
        .and_then(Value::as_str)
        .and_then(|target| target.strip_prefix('#'))
        .and_then(|path| root.pointer(path))
        .unwrap_or(root)
}

/// Whether masking `value` under `name` as a task's data changes it: a text holding a secret
/// (a URL's password), or a field below it named by a secret word.
fn holds_secret(name: &str, value: &Value) -> bool {
    let mut fields = serde_json::Map::new();
    fields.insert(name.to_owned(), value.clone());
    let original = Value::Object(fields);
    let mut masked = original.clone();
    mask_data(&mut masked);
    masked != original
}

/// What a field's type says about its input.
struct Shape {
    kind: ParamKind,
    nullable: bool,
    choices: Vec<Value>,
}

impl Shape {
    fn plain(kind: ParamKind) -> Self {
        Self {
            kind,
            nullable: false,
            choices: Vec::new(),
        }
    }

    fn choice(values: &[Value]) -> Self {
        Self {
            kind: ParamKind::Choice,
            nullable: values.iter().any(Value::is_null),
            choices: values.iter().filter(|v| !v.is_null()).cloned().collect(),
        }
    }

    /// A dataclass field's type.
    fn of_type(ty: &TypeDesc) -> Self {
        match ty {
            TypeDesc::Bool => Self::plain(ParamKind::Bool),
            TypeDesc::Int => Self::plain(ParamKind::Int),
            TypeDesc::Float => Self::plain(ParamKind::Float),
            TypeDesc::Str => Self::plain(ParamKind::Text),
            TypeDesc::Date => Self::plain(ParamKind::Date),
            TypeDesc::Datetime => Self::plain(ParamKind::Datetime),
            TypeDesc::Timedelta => Self::plain(ParamKind::Duration),
            TypeDesc::Literal { values } => Self::choice(values),
            TypeDesc::Union { variants } => {
                let others: Vec<&TypeDesc> = variants
                    .iter()
                    .filter(|v| !matches!(v, TypeDesc::None))
                    .collect();
                match others.as_slice() {
                    [one] if others.len() < variants.len() => {
                        let mut shape = Self::of_type(one);
                        if shape.kind != ParamKind::Json {
                            shape.nullable = true;
                        }
                        shape
                    }
                    _ => Self::plain(ParamKind::Json),
                }
            }
            _ => Self::plain(ParamKind::Json),
        }
    }

    /// A Pydantic model's property schema; `root` holds its `$defs`.
    fn of_schema(schema: &Value, root: &Value, depth: u8) -> Self {
        // A `$ref` to a `$ref`... is followed a few steps; a loop is JSON.
        if depth > 8 {
            return Self::plain(ParamKind::Json);
        }
        if let Some(target) = schema.get("$ref").and_then(Value::as_str) {
            return match target
                .strip_prefix("#/")
                .and_then(|path| root.pointer(&format!("/{path}")))
            {
                Some(found) => Self::of_schema(found, root, depth + 1),
                None => Self::plain(ParamKind::Json),
            };
        }
        if let Some(values) = schema.get("enum").and_then(Value::as_array) {
            return Self::choice(values);
        }
        if let Some(value) = schema.get("const") {
            return Self::choice(std::slice::from_ref(value));
        }
        if let Some(variants) = schema.get("anyOf").and_then(Value::as_array) {
            let others: Vec<&Value> = variants
                .iter()
                .filter(|v| v.get("type").and_then(Value::as_str) != Some("null"))
                .collect();
            return match others.as_slice() {
                [one] if others.len() < variants.len() => {
                    let mut shape = Self::of_schema(one, root, depth + 1);
                    if shape.kind != ParamKind::Json {
                        shape.nullable = true;
                    }
                    shape
                }
                _ => Self::plain(ParamKind::Json),
            };
        }
        let format = schema.get("format").and_then(Value::as_str);
        match schema.get("type").and_then(Value::as_str) {
            Some("boolean") => Self::plain(ParamKind::Bool),
            Some("integer") => Self::plain(ParamKind::Int),
            Some("number") => Self::plain(ParamKind::Float),
            Some("string") => Self::plain(match format {
                Some("date") => ParamKind::Date,
                Some("date-time") => ParamKind::Datetime,
                Some("duration") => ParamKind::Duration,
                _ => ParamKind::Text,
            }),
            _ => Self::plain(ParamKind::Json),
        }
    }
}
