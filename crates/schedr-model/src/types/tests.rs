use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::json;

use super::*;

fn rows() -> TypeDesc {
    TypeDesc::Dataclass {
        name: "Rows".into(),
        fields: vec![
            Field {
                name: "rows".into(),
                ty: TypeDesc::List {
                    item: Box::new(TypeDesc::Str),
                },
                required: true,
            },
            Field {
                name: "source".into(),
                ty: TypeDesc::Str,
                required: false,
            },
        ],
    }
}

#[rstest]
#[case(TypeDesc::Any, json!({"type": "any"}))]
#[case(TypeDesc::None, json!({"type": "none"}))]
#[case(TypeDesc::Datetime, json!({"type": "datetime"}))]
#[case(TypeDesc::ProcessResult, json!({"type": "process_result"}))]
#[case(
    TypeDesc::List { item: Box::new(TypeDesc::Int) },
    json!({"type": "list", "item": {"type": "int"}})
)]
#[case(
    TypeDesc::Tuple { items: vec![TypeDesc::Int, TypeDesc::Str] },
    json!({"type": "tuple", "items": [{"type": "int"}, {"type": "str"}]})
)]
#[case(
    TypeDesc::Dict { value: Box::new(TypeDesc::Float) },
    json!({"type": "dict", "value": {"type": "float"}})
)]
#[case(
    TypeDesc::Literal { values: vec![json!("eu"), json!(1), json!(null)] },
    json!({"type": "literal", "values": ["eu", 1, null]})
)]
#[case(
    TypeDesc::Union { variants: vec![TypeDesc::Int, TypeDesc::None] },
    json!({"type": "union", "variants": [{"type": "int"}, {"type": "none"}]})
)]
#[case(
    rows(),
    json!({"type": "dataclass", "name": "Rows", "fields": [
        {"name": "rows", "type": {"type": "list", "item": {"type": "str"}}, "required": true},
        {"name": "source", "type": {"type": "str"}, "required": false},
    ]})
)]
#[case(
    TypeDesc::TypedDict {
        name: "Row".into(),
        fields: vec![Field { name: "id".into(), ty: TypeDesc::Int, required: true }],
    },
    json!({"type": "typed_dict", "name": "Row", "fields": [
        {"name": "id", "type": {"type": "int"}, "required": true},
    ]})
)]
fn serializes_with_a_type_tag(#[case] ty: TypeDesc, #[case] expected: serde_json::Value) {
    assert_eq!(serde_json::to_value(&ty).unwrap(), expected);
    assert_eq!(serde_json::from_value::<TypeDesc>(expected).unwrap(), ty);
}

#[test]
fn rejects_unknown_fields_in_variants_with_fields() {
    assert!(
        serde_json::from_value::<TypeDesc>(
            json!({"type": "list", "item": {"type": "int"}, "x": 1})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<Field>(
            json!({"name": "a", "type": {"type": "int"}, "required": true, "x": 1})
        )
        .is_err()
    );
}

/// serde can't reject extra fields on unit variants of a tagged enum; the JSON Schema does
/// (`additionalProperties: false`), and Python's output is validated against it.
#[test]
fn unit_variants_ignore_extra_fields_but_the_schema_forbids_them() {
    let parsed: TypeDesc = serde_json::from_value(json!({"type": "int", "x": 1})).unwrap();
    assert_eq!(parsed, TypeDesc::Int);
    let schema = serde_json::to_value(schemars::schema_for!(TypeDesc)).unwrap();
    let int_variant = schema["oneOf"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["properties"]["type"]["const"] == "int")
        .unwrap();
    assert_eq!(int_variant["additionalProperties"], false);
}

#[rstest]
#[case("rows", Some(TypeDesc::List { item: Box::new(TypeDesc::Str) }))]
#[case("source", Some(TypeDesc::Str))]
#[case("missing", None)]
fn field_of_a_dataclass(#[case] name: &str, #[case] expected: Option<TypeDesc>) {
    assert_eq!(rows().field(name).cloned(), expected);
}

#[test]
fn field_of_a_typed_dict() {
    let ty = TypeDesc::TypedDict {
        name: "Row".into(),
        fields: vec![Field {
            name: "id".into(),
            ty: TypeDesc::Int,
            required: false,
        }],
    };
    assert_eq!(ty.field("id"), Some(&TypeDesc::Int));
    assert_eq!(ty.field("other"), None);
}

#[rstest]
#[case("stdout", Some(TypeDesc::Str))]
#[case("stderr", Some(TypeDesc::Str))]
#[case("exit_code", Some(TypeDesc::Int))]
#[case("stdout_truncated", Some(TypeDesc::Bool))]
#[case("stderr_truncated", Some(TypeDesc::Bool))]
#[case("output", None)]
fn field_of_a_process_result(#[case] name: &str, #[case] expected: Option<TypeDesc>) {
    assert_eq!(TypeDesc::ProcessResult.field(name).cloned(), expected);
}

#[test]
fn field_of_a_dict_is_its_value_type_and_any_stays_any() {
    let dict = TypeDesc::Dict {
        value: Box::new(TypeDesc::Float),
    };
    assert_eq!(dict.field("anything"), Some(&TypeDesc::Float));
    assert_eq!(TypeDesc::Any.field("anything"), Some(&TypeDesc::Any));
}

#[rstest]
#[case(TypeDesc::Int)]
#[case(TypeDesc::List { item: Box::new(TypeDesc::Int) })]
#[case(TypeDesc::Union { variants: vec![TypeDesc::Int] })]
fn types_without_named_fields_have_no_field(#[case] ty: TypeDesc) {
    assert_eq!(ty.field("x"), None);
}

#[test]
fn element_of_lists_tuples_and_any() {
    let list = TypeDesc::List {
        item: Box::new(TypeDesc::Str),
    };
    let tuple = TypeDesc::Tuple {
        items: vec![TypeDesc::Int, TypeDesc::Str],
    };
    assert_eq!(list.element(7), Some(&TypeDesc::Str));
    assert_eq!(tuple.element(0), Some(&TypeDesc::Int));
    assert_eq!(tuple.element(1), Some(&TypeDesc::Str));
    assert_eq!(tuple.element(2), None);
    assert_eq!(TypeDesc::Any.element(3), Some(&TypeDesc::Any));
}

#[rstest]
#[case(TypeDesc::Str)]
#[case(TypeDesc::Dict { value: Box::new(TypeDesc::Int) })]
#[case(rows())]
fn types_without_elements_have_no_element(#[case] ty: TypeDesc) {
    assert_eq!(ty.element(0), None);
}
