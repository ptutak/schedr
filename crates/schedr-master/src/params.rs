//! A DAG's run parameters (decisions 133, 135, 138): the check of its schema when the DAG is
//! validated, and of a run's values before the run exists.
//!
//! The master runs no user code: it checks values against the JSON Schema the runner wrote
//! (with the `jsonschema` crate), and refuses at validation a schema asking for a check that
//! crate doesn't make, so what passes here never falls short of the schema. Pydantic's own
//! validators run only when a task decodes the values.

use jsonschema::error::{TypeKind, ValidationErrorKind};
use jsonschema::{Draft, Validator};
use schedr_config::masked::{mask_text, names_secret_in_words};
use schedr_model::Params;
use schedr_model::params::{MAX_PARAMS_BYTES, duration_micros, to_stored};
use serde_json::{Map, Value};

#[cfg(test)]
mod tests;

/// Keywords the validator checks (JSON Schema 2020-12, and `definitions` of the drafts
/// Pydantic once wrote).
const CHECKED: &[&str] = &[
    "type",
    "enum",
    "const",
    "multipleOf",
    "maximum",
    "exclusiveMaximum",
    "minimum",
    "exclusiveMinimum",
    "maxLength",
    "minLength",
    "pattern",
    "maxItems",
    "minItems",
    "uniqueItems",
    "maxContains",
    "minContains",
    "maxProperties",
    "minProperties",
    "required",
    "dependentRequired",
    "allOf",
    "anyOf",
    "oneOf",
    "not",
    "if",
    "then",
    "else",
    "dependentSchemas",
    "prefixItems",
    "items",
    "contains",
    "properties",
    "patternProperties",
    "additionalProperties",
    "propertyNames",
    "unevaluatedItems",
    "unevaluatedProperties",
    "format",
    "$ref",
    "$defs",
    "definitions",
];

/// Keywords that only describe (nothing to check): `discriminator` is Pydantic's hint for a
/// union its `oneOf` already checks.
const ANNOTATIONS: &[&str] = &[
    "$schema",
    "$comment",
    "title",
    "description",
    "default",
    "examples",
    "deprecated",
    "readOnly",
    "writeOnly",
    "discriminator",
];

/// The formats the validator checks (without its `idna` feature: no `idn-*`).
const FORMATS: &[&str] = &[
    "date",
    "date-time",
    "time",
    "duration",
    "email",
    "hostname",
    "ipv4",
    "ipv6",
    "uri",
    "uri-reference",
    "iri",
    "iri-reference",
    "uri-template",
    "uuid",
    "json-pointer",
    "relative-json-pointer",
    "regex",
];

/// Keywords whose value is one schema.
const ONE_SCHEMA: &[&str] = &[
    "not",
    "if",
    "then",
    "else",
    "items",
    "contains",
    "additionalProperties",
    "propertyNames",
    "unevaluatedItems",
    "unevaluatedProperties",
];

/// Keywords whose value is a list of schemas.
const SCHEMA_LIST: &[&str] = &["allOf", "anyOf", "oneOf", "prefixItems"];

/// Keywords whose value maps names to schemas.
const SCHEMA_MAP: &[&str] = &[
    "properties",
    "patternProperties",
    "dependentSchemas",
    "$defs",
    "definitions",
];

/// What is wrong with a DAG's `params`, for the DAG's validation; empty when nothing is.
/// `scheduled`: the DAG has a schedule, so every field needs a default.
#[must_use]
pub fn problems(params: &Params, scheduled: bool) -> Vec<String> {
    let mut problems = Vec::new();
    if !params.schema.is_object() {
        return vec!["params: the schema isn't an object".to_owned()];
    }
    keyword_problems(&params.schema, "", &mut problems);
    if !problems.is_empty() {
        return problems;
    }
    if !matches!(
        params.ty,
        schedr_model::TypeDesc::Dataclass { .. } | schedr_model::TypeDesc::Model { .. }
    ) {
        problems.push("params: must be a dataclass or a Pydantic model".to_owned());
    }
    if scheduled
        && let Some(missing) = required(&params.schema)
            .into_iter()
            .find(|name| !params.defaults.contains_key(*name))
    {
        problems.push(format!(
            "params: `{missing}` has no default, but the DAG has a schedule (its runs use the \
             defaults)"
        ));
    }
    if let Err(why) = defaults_checked(params) {
        problems.push(format!("params: the defaults: {why}"));
    }
    problems
}

/// The fields the schema requires.
fn required(schema: &Value) -> Vec<&str> {
    fields_schema(schema)
        .get("required")
        .and_then(Value::as_array)
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

fn defaults_checked(params: &Params) -> Result<(), String> {
    validator(&params.schema)?;
    let size = Value::Object(params.defaults.clone()).to_string().len();
    if size > MAX_PARAMS_BYTES {
        return Err(over_limit(size));
    }
    // Each default alone, against its field's schema (the others may be required and without
    // one), with the definitions its `$ref`s name.
    for (name, value) in &params.defaults {
        let property = properties(&params.schema)
            .and_then(|p| p.get(name))
            .ok_or_else(|| format!("`{name}` isn't a field"))?;
        let mut one = Map::new();
        for defs in ["$defs", "definitions"] {
            if let Some(found) = params.schema.get(defs) {
                one.insert(defs.to_owned(), found.clone());
            }
        }
        one.insert("properties".to_owned(), json_object(name, property.clone()));
        check(
            &validator(&Value::Object(one))?,
            &json_object(name, value.clone()),
        )?;
    }
    Ok(())
}

fn json_object(name: &str, value: Value) -> Value {
    let mut map = Map::new();
    map.insert(name.to_owned(), value);
    Value::Object(map)
}

/// A run's parameters, as stored (decision 138): the DAG's defaults with `given` over them,
/// checked against its schema.
///
/// # Errors
///
/// Why they are refused, to show: parameters for a DAG that takes none, an unknown field,
/// values that don't fit the schema, or over [`MAX_PARAMS_BYTES`].
pub fn run_params(
    params: Option<&Params>,
    given: Option<&Map<String, Value>>,
) -> Result<Value, String> {
    let given = given.filter(|g| !g.is_empty());
    let Some(params) = params else {
        return match given {
            Some(_) => Err("the DAG takes no parameters (it declares no `params=`)".to_owned()),
            None => Ok(Value::Object(Map::new())),
        };
    };
    let mut values = params.defaults.clone();
    if let Some(given) = given {
        let known = properties(&params.schema);
        for (name, value) in given {
            if !known.is_some_and(|k| k.contains_key(name)) {
                let names = known
                    .map(|k| {
                        k.keys()
                            .map(|n| format!("`{n}`"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                // The name is the trigger's, as typed: masked as a text.
                return Err(format!(
                    "unknown parameter `{}` (the DAG takes {names})",
                    mask_text(name)
                ));
            }
            values.insert(name.clone(), value.clone());
        }
    }
    let values = Value::Object(values);
    let size = values.to_string().len();
    if size > MAX_PARAMS_BYTES {
        return Err(over_limit(size));
    }
    check(&validator(&params.schema)?, &values)?;
    // Its reason never quotes the value; under a secret name, it doesn't give one either.
    to_stored(&values, &params.ty).map_err(|misfit| {
        if misfit.fields().any(names_secret_in_words) {
            format!(
                "`{}`: doesn't fit its type (the value isn't shown)",
                mask_text(&misfit.at())
            )
        } else {
            // Names in it (a key, an unknown field) are the trigger's, as typed.
            mask_text(&misfit.to_string())
        }
    })
}

/// The object schema whose `properties` are the parameters: the root, or what its `$ref`
/// names (Pydantic's form for a model that refers to itself).
fn fields_schema(schema: &Value) -> &Value {
    schema
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|r| r.strip_prefix('#'))
        .and_then(|pointer| schema.pointer(pointer))
        .unwrap_or(schema)
}

/// The parameters' schemas, by name.
fn properties(schema: &Value) -> Option<&Map<String, Value>> {
    fields_schema(schema)
        .get("properties")
        .and_then(Value::as_object)
}

fn over_limit(size: usize) -> String {
    format!(
        "the parameters are {size} bytes as JSON, over the limit of {} KiB",
        MAX_PARAMS_BYTES / 1024
    )
}

fn validator(schema: &Value) -> Result<Validator, String> {
    jsonschema::options()
        .with_draft(Draft::Draft202012)
        .should_validate_formats(true)
        // ISO 8601, fractions of a second included (`PT0.5S`, Pydantic's form; RFC 3339's
        // `duration`, the crate's, has none), not in years or months (decision 141).
        .with_format("duration", is_duration)
        .build(schema)
        .map_err(|e| format!("the schema is invalid: {e}"))
}

/// Every reason `values` don't fit, each with where (`limits/1: ...`). A reason never quotes
/// a value nor the schema's values (decision 134): it names the keyword that failed, a
/// missing or unknown field, the expected type or the format; names are masked as texts, and
/// under a field named by a secret word not even the keyword is given.
fn check(validator: &Validator, values: &Value) -> Result<(), String> {
    let reasons: Vec<String> = validator
        .iter_errors(values)
        .map(|e| {
            let path = pointer_steps(&e.instance_path().to_string());
            let at = path
                .iter()
                .map(|step| mask_text(step))
                .collect::<Vec<_>>()
                .join("/");
            let reason = if path.iter().any(|step| names_secret_in_words(step)) {
                "doesn't fit its schema (the value isn't shown)".to_owned()
            } else {
                reason(e.kind())
            };
            if at.is_empty() {
                reason
            } else {
                format!("`{at}`: {reason}")
            }
        })
        .collect();
    if reasons.is_empty() {
        Ok(())
    } else {
        Err(reasons.join("; "))
    }
}

/// Why a value doesn't fit, from what failed only: nothing of the value, nor a value the
/// schema holds (a `const`, an `enum`, a `not`'s schema).
fn reason(kind: &ValidationErrorKind) -> String {
    let names = |names: &[String]| {
        names
            .iter()
            .map(|n| format!("`{}`", mask_text(n)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match kind {
        ValidationErrorKind::Required { property } => match property.as_str() {
            Some(name) => format!("`{}` is required", mask_text(name)),
            None => "a required field is missing".to_owned(),
        },
        ValidationErrorKind::Type {
            kind: TypeKind::Single(ty),
        } => format!("not of type `{ty}`"),
        ValidationErrorKind::Type { .. } => "not of any of its types".to_owned(),
        ValidationErrorKind::Format { format } => format!("not of the format `{format}`"),
        ValidationErrorKind::AdditionalProperties { unexpected }
        | ValidationErrorKind::UnevaluatedProperties { unexpected } => {
            format!("unknown fields {}", names(unexpected))
        }
        other => format!("doesn't fit the schema's `{}`", other.keyword()),
    }
}

/// A JSON pointer's steps as the names they are (`api~1key` is `api/key`).
fn pointer_steps(pointer: &str) -> Vec<String> {
    pointer
        .split('/')
        .skip(1)
        .map(|step| step.replace("~1", "/").replace("~0", "~"))
        .collect()
}

/// Every keyword of `schema` (at `at`, a JSON pointer) the validator doesn't check, or a format
/// it doesn't know, or a `$ref` it would have to fetch.
fn keyword_problems(schema: &Value, at: &str, problems: &mut Vec<String>) {
    let Value::Object(object) = schema else {
        if !schema.is_boolean() {
            problems.push(format!(
                "params: the schema at `{}` isn't a schema",
                shown(at)
            ));
        }
        return;
    };
    for (keyword, value) in object {
        let here = format!("{at}/{keyword}");
        if !CHECKED.contains(&keyword.as_str()) && !ANNOTATIONS.contains(&keyword.as_str()) {
            problems.push(format!(
                "params: the schema's keyword `{keyword}` (at `{}`) isn't one the master checks; \
                 remove it, or check it in the task",
                shown(at)
            ));
            continue;
        }
        match keyword.as_str() {
            "format" => {
                if let Some(format) = value.as_str()
                    && !FORMATS.contains(&format)
                {
                    problems.push(format!(
                        "params: the format `{format}` (at `{}`) isn't one the master checks \
                         ({}); use another type, or check it in the task",
                        shown(at),
                        FORMATS.join(", ")
                    ));
                }
            }
            "$ref" => {
                if !value.as_str().is_some_and(|r| r.starts_with('#')) {
                    problems.push(format!(
                        "params: `$ref` {value} (at `{}`) isn't within the schema; the master \
                         fetches nothing",
                        shown(at)
                    ));
                }
            }
            "$schema" => {
                if value.as_str().is_some_and(|s| !s.contains("2020-12")) {
                    problems.push(format!(
                        "params: `$schema` {value}: the master checks JSON Schema 2020-12"
                    ));
                }
            }
            k if ONE_SCHEMA.contains(&k) => keyword_problems(value, &here, problems),
            k if SCHEMA_LIST.contains(&k) => {
                for (i, item) in value.as_array().into_iter().flatten().enumerate() {
                    keyword_problems(item, &format!("{here}/{i}"), problems);
                }
            }
            k if SCHEMA_MAP.contains(&k) => {
                for (name, item) in value.as_object().into_iter().flatten() {
                    keyword_problems(item, &format!("{here}/{name}"), problems);
                }
            }
            _ => {}
        }
    }
}

/// The rule a dataclass's duration is stored by (decision 139): ISO 8601, not in years or
/// months.
fn is_duration(text: &str) -> bool {
    duration_micros(text).is_ok()
}

fn shown(at: &str) -> &str {
    if at.is_empty() { "/" } else { at }
}
