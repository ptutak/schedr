use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn fields(params: &Value) -> Value {
    let params: Params = serde_json::from_value(params.clone()).unwrap();
    serde_json::to_value(ParamFieldView::of(&params)).unwrap()
}

fn field(name: &str, ty: &Value, required: bool) -> Value {
    json!({"name": name, "type": ty, "required": required})
}

#[test]
fn a_dataclass_field_is_entered_by_its_type() {
    let none = json!({"type": "none"});
    let params = json!({
        "type": {"type": "dataclass", "name": "P", "fields": [
            field("region", &json!({"type": "str"}), true),
            field("rows", &json!({"type": "int"}), false),
            field("ratio", &json!({"type": "float"}), false),
            field("full", &json!({"type": "bool"}), false),
            field("day", &json!({"type": "union", "variants": [{"type": "date"}, none]}), false),
            field("at", &json!({"type": "datetime"}), false),
            field("every", &json!({"type": "timedelta"}), false),
            field("mode", &json!({"type": "literal", "values": ["fast", 2, null]}), false),
            field("tags", &json!({"type": "list", "item": {"type": "str"}}), false),
            field("either", &json!({"type": "union", "variants": [{"type": "int"}, {"type": "str"}]}), false),
        ]},
        "schema": {"type": "object", "required": ["region"], "properties": {}},
        "defaults": {
            "rows": 10, "ratio": 0.5, "full": false, "day": null, "at": "2026-10-15T03:00:00Z",
            "every": "PT1H", "mode": "fast", "tags": ["a"], "either": 1
        },
    });
    assert_eq!(
        fields(&params),
        json!([
            {"name": "region", "kind": "text", "required": true, "nullable": false, "secret": false},
            {"name": "rows", "kind": "int", "required": false, "nullable": false, "secret": false, "default": 10},
            {"name": "ratio", "kind": "float", "required": false, "nullable": false, "secret": false, "default": 0.5},
            {"name": "full", "kind": "bool", "required": false, "nullable": false, "secret": false, "default": false},
            {"name": "day", "kind": "date", "required": false, "nullable": true, "secret": false, "default": null},
            {"name": "at", "kind": "datetime", "required": false, "nullable": false, "secret": false, "default": "2026-10-15T03:00:00Z"},
            {"name": "every", "kind": "duration", "required": false, "nullable": false, "secret": false, "default": "PT1H"},
            {"name": "mode", "kind": "choice", "required": false, "nullable": true, "secret": false, "default": "fast", "choices": ["fast", 2]},
            {"name": "tags", "kind": "json", "required": false, "nullable": false, "secret": false, "default": ["a"]},
            {"name": "either", "kind": "json", "required": false, "nullable": false, "secret": false, "default": 1},
        ])
    );
}

#[test]
fn a_secret_fields_default_and_choices_are_withheld() {
    let params = json!({
        "type": {"type": "dataclass", "name": "P", "fields": [
            field("api_token", &json!({"type": "str"}), false),
            field("db_password", &json!({"type": "literal", "values": ["hunter2"]}), false),
            field("source", &json!({"type": "str"}), false),
            field("target", &json!({"type": "literal", "values": ["postgres://u:hunter4@db/x"]}), false),
            field("settings", &json!({"type": "dict", "value": {"type": "str"}}), false),
            field("partition_key", &json!({"type": "str"}), false),
        ]},
        "schema": {"type": "object", "properties": {}},
        "defaults": {
            "api_token": "hunter1", "db_password": "hunter2",
            "source": "postgres://u:hunter3@db/x", "target": "postgres://u:hunter4@db/x",
            "settings": {"password": "hunter5"}, "partition_key": "day"
        },
    });
    let shown = fields(&params);
    let text = shown.to_string();
    for secret in ["hunter1", "hunter2", "hunter3", "hunter4", "hunter5"] {
        assert!(!text.contains(secret), "{secret} in {text}");
    }
    let secret: Vec<(&str, bool)> = shown
        .as_array()
        .unwrap()
        .iter()
        .map(|f| (f["name"].as_str().unwrap(), f["secret"].as_bool().unwrap()))
        .collect();
    assert_eq!(
        secret,
        [
            ("api_token", true),
            ("db_password", true),
            ("source", true),
            ("target", true),
            ("settings", true),
            ("partition_key", false),
        ]
    );
    assert_eq!(shown[5]["default"], "day");
}

#[test]
fn a_models_fields_are_read_from_its_schema() {
    let params = json!({
        "type": {"type": "model", "name": "P", "schema": {}},
        "schema": {
            "type": "object",
            "required": ["region", "level"],
            "properties": {
                "region": {"type": "string"},
                "level": {"$ref": "#/$defs/Level"},
                "day": {"anyOf": [{"type": "string", "format": "date"}, {"type": "null"}], "default": null},
                "every": {"type": "string", "format": "duration", "default": "PT1H"},
                "count": {"type": "integer", "default": 3},
                "inner": {"$ref": "#/$defs/Inner"},
                "loop": {"$ref": "#/$defs/Loop"},
                "fixed": {"const": "x"},
            },
            "$defs": {
                "Level": {"enum": ["low", "high"], "type": "string"},
                "Inner": {"type": "object", "properties": {"a": {"type": "integer"}}},
                "Loop": {"$ref": "#/$defs/Loop"},
            },
        },
        "defaults": {"day": null, "every": "PT1H", "count": 3, "inner": {"a": 1}},
    });
    let kinds: Vec<(String, String, bool, bool)> = fields(&params)
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["name"].as_str().unwrap().to_owned(),
                f["kind"].as_str().unwrap().to_owned(),
                f["required"].as_bool().unwrap(),
                f["nullable"].as_bool().unwrap(),
            )
        })
        .collect();
    let expected = [
        ("count", "int", false, false),
        ("day", "date", false, true),
        ("every", "duration", false, false),
        ("fixed", "choice", false, false),
        ("inner", "json", false, false),
        ("level", "choice", true, false),
        ("loop", "json", false, false),
        ("region", "text", true, false),
    ];
    assert_eq!(
        kinds,
        expected
            .iter()
            .map(|(n, k, r, o)| ((*n).to_owned(), (*k).to_owned(), *r, *o))
            .collect::<Vec<_>>()
    );
    assert_eq!(fields(&params)[5]["choices"], json!(["low", "high"]));
}

#[test]
fn a_null_default_reads_back_as_a_default() {
    let view: ParamFieldView = serde_json::from_value(json!({
        "name": "day", "kind": "date", "required": false, "nullable": true, "secret": false,
        "default": null
    }))
    .unwrap();
    assert_eq!(view.default, Some(Value::Null));
    let view: ParamFieldView = serde_json::from_value(json!({
        "name": "day", "kind": "date", "required": true, "nullable": true, "secret": false
    }))
    .unwrap();
    assert_eq!(view.default, None);
}

#[test]
fn a_recursive_models_fields_are_read_through_its_root_ref() {
    let params = json!({
        "type": {"type": "model", "name": "P", "schema": {}},
        "schema": {
            "$ref": "#/$defs/P",
            "$defs": {"P": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"},
                    "rows": {"type": "integer", "default": 10},
                    "child": {"anyOf": [{"$ref": "#/$defs/P"}, {"type": "null"}], "default": null},
                },
            }},
        },
        "defaults": {"rows": 10, "child": null},
    });
    let shown: Vec<(String, String, bool)> = fields(&params)
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["name"].as_str().unwrap().to_owned(),
                f["kind"].as_str().unwrap().to_owned(),
                f["required"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        shown,
        [
            ("child".to_owned(), "json".to_owned(), false),
            ("name".to_owned(), "text".to_owned(), true),
            ("rows".to_owned(), "int".to_owned(), false),
        ]
    );
}
