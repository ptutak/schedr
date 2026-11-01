use pretty_assertions::assert_eq;
use rstest::rstest;
use schedr_model::{Field, TypeDesc};
use serde_json::{Value, json};

use super::*;

fn field(name: &str, ty: TypeDesc, required: bool) -> Field {
    Field {
        name: name.to_owned(),
        ty,
        required,
    }
}

/// `ReportParams` as the runner writes it: region (`Literal`), full, day (`date | None`),
/// and a required `name` of at least one character.
fn report() -> Params {
    Params {
        ty: TypeDesc::Dataclass {
            name: "ReportParams".to_owned(),
            fields: vec![
                field(
                    "region",
                    TypeDesc::Literal {
                        values: vec![json!("eu"), json!("us")],
                    },
                    false,
                ),
                field("full", TypeDesc::Bool, false),
                field(
                    "day",
                    TypeDesc::Union {
                        variants: vec![TypeDesc::Date, TypeDesc::None],
                    },
                    false,
                ),
                field("name", TypeDesc::Str, true),
            ],
        },
        schema: json!({
            "type": "object",
            "title": "ReportParams",
            "properties": {
                "region": {"enum": ["eu", "us"], "default": "eu"},
                "full": {"type": "boolean", "default": false},
                "day": {"anyOf": [{"type": "string", "format": "date"}, {"type": "null"}]},
                "name": {"type": "string", "minLength": 1},
            },
            "required": ["name"],
            "additionalProperties": false,
        }),
        defaults: json!({"region": "eu", "full": false, "day": null})
            .as_object()
            .unwrap()
            .clone(),
    }
}

fn given(values: &Value) -> Map<String, Value> {
    values.as_object().unwrap().clone()
}

#[test]
fn a_triggers_values_go_over_the_defaults_and_are_stored_typed() {
    let stored = run_params(
        Some(&report()),
        Some(&given(
            &json!({"region": "us", "day": "2026-10-01", "name": "q3"}),
        )),
    )
    .unwrap();
    assert_eq!(
        stored,
        json!({"region": "us", "full": false, "day": {"$date": "2026-10-01"}, "name": "q3"})
    );
}

#[rstest]
#[case::enum_value(json!({"name": "x", "region": "asia"}), "`region`: ")]
#[case::wrong_type(json!({"name": "x", "full": "yes"}), "`full`: ")]
#[case::bad_date(json!({"name": "x", "day": "2026-13-45"}), "`day`: ")]
#[case::constraint(json!({"name": ""}), "`name`: ")]
#[case::missing(json!({"region": "us"}), "`name` is required")]
#[case::unknown(json!({"name": "x", "nope": 1}), "unknown parameter `nope` (the DAG takes `day`, `full`, `name`, `region`)")]
fn values_that_dont_fit_the_schema_are_refused_with_the_reason(
    #[case] values: Value,
    #[case] reason: &str,
) {
    let error = run_params(Some(&report()), Some(&given(&values))).unwrap_err();
    assert!(error.contains(reason), "{error}");
}

#[test]
fn a_dag_without_params_takes_none() {
    assert_eq!(run_params(None, None).unwrap(), json!({}));
    assert_eq!(run_params(None, Some(&Map::new())).unwrap(), json!({}));
    let error = run_params(None, Some(&given(&json!({"a": 1})))).unwrap_err();
    assert!(error.contains("takes no parameters"), "{error}");
}

#[test]
fn params_over_64_kib_are_refused() {
    let big = "x".repeat(MAX_PARAMS_BYTES);
    let error = run_params(Some(&report()), Some(&given(&json!({"name": big})))).unwrap_err();
    assert!(error.contains("over the limit of 64 KiB"), "{error}");
}

#[test]
fn a_valid_params_declaration_has_no_problems() {
    assert_eq!(problems(&report(), false), Vec::<String>::new());
}

#[test]
fn a_scheduled_dag_needs_a_default_for_every_field() {
    assert_eq!(
        problems(&report(), true),
        ["params: `name` has no default, but the DAG has a schedule (its runs use the defaults)"]
    );
}

#[rstest]
#[case::unknown_keyword(json!({"x-ui": 1}), "the schema's keyword `x-ui` (at `/properties/name`) isn't one the master checks")]
#[case::unknown_format(json!({"format": "path"}), "the format `path` (at `/properties/name`) isn't one the master checks")]
#[case::idn_format(json!({"format": "idn-email"}), "the format `idn-email`")]
#[case::remote_ref(json!({"$ref": "https://example.com/s.json"}), "isn't within the schema; the master fetches nothing")]
#[case::content(json!({"contentEncoding": "base64"}), "`contentEncoding`")]
fn a_schema_asking_for_a_check_the_master_doesnt_make_is_a_problem(
    #[case] extra: Value,
    #[case] problem: &str,
) {
    let mut params = report();
    let name = &mut params.schema["properties"]["name"];
    for (k, v) in extra.as_object().unwrap() {
        name[k] = v.clone();
    }
    let found = problems(&params, false);
    assert!(found.iter().any(|p| p.contains(problem)), "{found:?}");
}

#[test]
fn keywords_are_checked_in_nested_schemas_too() {
    let mut params = report();
    params.schema["$defs"] = json!({"Inner": {"anyOf": [{"type": "string", "x-nested": true}]}});
    let found = problems(&params, false);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("`x-nested` (at `/$defs/Inner/anyOf/0`)"));
}

#[test]
fn a_default_that_doesnt_fit_its_field_is_a_problem() {
    let mut params = report();
    params.defaults.insert("region".to_owned(), json!("asia"));
    let found = problems(&params, false);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(
        found[0].starts_with("params: the defaults: `region`: "),
        "{found:?}"
    );
}

#[test]
fn pydantic_schemas_with_local_refs_and_discriminators_pass() {
    let params = Params {
        ty: TypeDesc::Model {
            name: "P".to_owned(),
            schema: json!({}),
        },
        schema: json!({
            "$defs": {"Eu": {"properties": {"k": {"const": "eu", "type": "string"}},
                             "required": ["k"], "title": "Eu", "type": "object"}},
            "properties": {"target": {"discriminator": {"mapping": {"eu": "#/$defs/Eu"},
                                                        "propertyName": "k"},
                                      "oneOf": [{"$ref": "#/$defs/Eu"}], "title": "Target"}},
            "required": ["target"], "title": "P", "type": "object",
        }),
        defaults: Map::new(),
    };
    assert_eq!(problems(&params, false), Vec::<String>::new());
    let stored = run_params(Some(&params), Some(&given(&json!({"target": {"k": "eu"}})))).unwrap();
    assert_eq!(stored, json!({"target": {"k": "eu"}}));
    let error =
        run_params(Some(&params), Some(&given(&json!({"target": {"k": "us"}})))).unwrap_err();
    assert!(error.contains("`target"), "{error}");
}

/// A dataclass with a duration whose name is a secret word: its schema is plain text, so a
/// bad value is refused when it is stored, not by the schema.
fn with_ttl() -> Params {
    Params {
        ty: TypeDesc::Dataclass {
            name: "P".to_owned(),
            fields: vec![field("token_ttl", TypeDesc::Timedelta, false)],
        },
        schema: json!({
            "type": "object",
            "properties": {"token_ttl": {"type": "string", "default": "P0DT60.000000S"}},
            "additionalProperties": false,
        }),
        defaults: given(&json!({"token_ttl": "P0DT60.000000S"})),
    }
}

#[test]
fn a_value_refused_when_stored_is_not_shown_under_a_secret_name() {
    let error = run_params(
        Some(&with_ttl()),
        Some(&given(&json!({"token_ttl": "super-secret-token-value"}))),
    )
    .unwrap_err();
    assert!(!error.contains("super-secret"), "{error}");
    assert_eq!(
        error,
        "`token_ttl`: doesn't fit its type (the value isn't shown)"
    );
}

/// `PT1H0.5S`: how Pydantic writes `timedelta(hours=1, milliseconds=500)` with `format: duration`.
#[test]
fn a_models_fractional_duration_fits_its_format() {
    let params = Params {
        ty: TypeDesc::Model {
            name: "P".to_owned(),
            schema: json!({}),
        },
        schema: json!({
            "type": "object",
            "properties": {"window": {"type": "string", "format": "duration", "default": "PT1H0.5S"}},
        }),
        defaults: given(&json!({"window": "PT1H0.5S"})),
    };
    assert_eq!(problems(&params, true), Vec::<String>::new());
    assert_eq!(
        run_params(Some(&params), None).unwrap(),
        json!({"window": "PT1H0.5S"})
    );
    let error = run_params(Some(&params), Some(&given(&json!({"window": "an hour"})))).unwrap_err();
    assert!(error.contains("not of the format `duration`"), "{error}");
}

/// What Pydantic writes for a model that refers to itself: the root is a `$ref`.
#[test]
fn a_schema_whose_root_is_a_ref_has_its_fields() {
    let params = Params {
        ty: TypeDesc::Model {
            name: "Node".to_owned(),
            schema: json!({}),
        },
        schema: json!({
            "$defs": {"Node": {
                "type": "object",
                "title": "Node",
                "properties": {
                    "name": {"type": "string", "default": "n"},
                    "child": {"anyOf": [{"$ref": "#/$defs/Node"}, {"type": "null"}], "default": null},
                },
            }},
            "$ref": "#/$defs/Node",
        }),
        defaults: given(&json!({"name": "n", "child": null})),
    };
    assert_eq!(problems(&params, true), Vec::<String>::new());
    assert_eq!(
        run_params(Some(&params), Some(&given(&json!({"name": "x"})))).unwrap(),
        json!({"name": "x", "child": null})
    );
    let error = run_params(Some(&params), Some(&given(&json!({"nope": 1})))).unwrap_err();
    assert!(error.contains("(the DAG takes `child`, `name`)"), "{error}");
}

/// A model's params with `schema` as the properties, for the masking tests.
fn model_with(properties: Value) -> Params {
    let mut schema = json!({"type": "object"});
    schema["properties"] = properties;
    Params {
        ty: TypeDesc::Model {
            name: "P".to_owned(),
            schema: json!({}),
        },
        schema,
        defaults: Map::new(),
    }
}

#[test]
fn a_reason_on_a_parent_hides_the_secrets_inside_it() {
    let params =
        model_with(json!({"config": {"oneOf": [{"type": "string"}, {"type": "integer"}]}}));
    let error = run_params(
        Some(&params),
        Some(&given(
            &json!({"config": {"db_password": ["hunter2", "tailsecret"]}}),
        )),
    )
    .unwrap_err();
    assert!(
        !error.contains("hunter2") && !error.contains("tailsecret"),
        "{error}"
    );
    assert!(error.starts_with("`config`: "), "{error}");
}

#[test]
fn a_reason_quoting_text_masks_it_as_text() {
    let params = model_with(json!({"value": {"type": "string", "pattern": "^allowed$"}}));
    let error = run_params(
        Some(&params),
        Some(&given(&json!({"value": "password=\"hunter2 tailsecret\""}))),
    )
    .unwrap_err();
    assert!(
        !error.contains("hunter2") && !error.contains("tailsecret"),
        "{error}"
    );
}

#[test]
fn a_secret_name_with_a_slash_is_still_a_secret_name() {
    let params = model_with(json!({"api/key": {"type": "integer"}}));
    let error =
        run_params(Some(&params), Some(&given(&json!({"api/key": "hunter2"})))).unwrap_err();
    assert_eq!(
        error,
        "`api/key`: doesn't fit its schema (the value isn't shown)"
    );
}

#[test]
fn a_secret_dict_key_with_a_dot_is_still_a_secret_name() {
    let params = Params {
        ty: TypeDesc::Dataclass {
            name: "P".to_owned(),
            fields: vec![field(
                "config",
                TypeDesc::Dict {
                    value: Box::new(TypeDesc::Timedelta),
                },
                false,
            )],
        },
        schema: json!({
            "type": "object",
            "properties": {"config": {"type": "object", "additionalProperties": {"type": "string"}}},
        }),
        defaults: Map::new(),
    };
    let error = run_params(
        Some(&params),
        Some(&given(&json!({"config": {"api.key": "hunter2"}}))),
    )
    .unwrap_err();
    assert!(!error.contains("hunter2"), "{error}");
}

#[rstest]
#[case::text(json!({"name": "hunter2\u{0}tail"}))]
#[case::key(json!({"name": "x", "tags": {"a\u{0}b": "c"}}))]
fn text_with_nul_is_refused_without_showing_it(#[case] values: Value) {
    let mut params = model_with(json!({
        "name": {"type": "string"},
        "tags": {"type": "object"},
    }));
    params.ty = TypeDesc::Any;
    let error = run_params(Some(&params), Some(&given(&values))).unwrap_err();
    assert!(error.contains("NUL"), "{error}");
    assert!(!error.contains("hunter2"), "{error}");
}

#[rstest]
#[case::months("P1M")]
#[case::years("P1Y")]
fn a_models_duration_in_months_or_years_is_refused(#[case] window: &str) {
    let params = model_with(json!({"window": {"type": "string", "format": "duration"}}));
    let error = run_params(Some(&params), Some(&given(&json!({"window": window})))).unwrap_err();
    assert!(error.contains("not of the format `duration`"), "{error}");
}

#[test]
fn a_default_failing_its_parent_schema_hides_the_secrets_inside_it() {
    let mut params =
        model_with(json!({"config": {"oneOf": [{"type": "string"}, {"type": "integer"}]}}));
    params.defaults.insert(
        "config".to_owned(),
        json!({"db_password": ["hunter2", "tailsecret"]}),
    );
    let found = problems(&params, false);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(
        !found[0].contains("hunter2") && !found[0].contains("tailsecret"),
        "{found:?}"
    );
}

#[rstest]
#[case::not(
    json!({"not": {"properties": {"db_password": {"const": "hunter2"}}}}),
    json!({"db_password": "hunter2"})
)]
#[case::const_(json!({"const": "password=\"hunter2 x\""}), json!("other"))]
#[case::enum_(json!({"enum": ["password=\"hunter2 x\""]}), json!("other"))]
#[case::property_names(json!({"propertyNames": {"maxLength": 1}}), json!({"password=\"hunter2\"": 1}))]
#[case::unique(json!({"uniqueItems": true}), json!(["hunter2", "hunter2"]))]
#[case::contains(json!({"contains": {"const": 1}}), json!(["hunter2"]))]
fn a_reason_quotes_neither_the_value_nor_the_schemas_values(
    #[case] schema: Value,
    #[case] value: Value,
) {
    let params = model_with(json!({"v": schema}));
    let error = run_params(Some(&params), Some(&given(&json!({"v": value})))).unwrap_err();
    assert!(!error.contains("hunter2"), "{error}");
    assert!(error.starts_with("`v`: "), "{error}");
}

#[test]
fn an_unknown_field_named_with_a_secret_is_masked() {
    let mut params = model_with(json!({"v": {"type": "object", "additionalProperties": false}}));
    params.schema["properties"]["v"]["properties"] = json!({});
    let error = run_params(
        Some(&params),
        Some(&given(&json!({"v": {"postgres://u:hunter2@db/x": 1}}))),
    )
    .unwrap_err();
    assert!(!error.contains("hunter2"), "{error}");
    assert!(
        error.contains("unknown fields `postgres://u:****@db/x`"),
        "{error}"
    );
}

#[test]
fn an_unknown_parameter_is_named_masked() {
    let error = run_params(
        Some(&report()),
        Some(&given(&json!({"postgres://u:hunter2@db/x": 1}))),
    )
    .unwrap_err();
    assert!(
        error.starts_with("unknown parameter `postgres://u:****@db/x`"),
        "{error}"
    );
}
