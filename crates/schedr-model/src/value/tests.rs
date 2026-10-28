use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::json;

use super::*;

fn task(id: &str) -> TaskId {
    TaskId::new(id).unwrap()
}

fn output(id: &str, path: Vec<PathElem>) -> Reference {
    Reference::Output {
        on: Vec::new(),
        task: task(id),
        path,
    }
}

fn planned_for() -> Reference {
    Reference::DagCtx {
        field: DagCtxField::PlannedFor,
    }
}

#[rstest]
#[case(output("extract", vec![]), json!({"kind": "output", "task": "extract"}))]
#[case(
    output("extract", vec![PathElem::Field("rows".into()), PathElem::Index(0)]),
    json!({"kind": "output", "task": "extract", "path": ["rows", 0]})
)]
#[case(planned_for(), json!({"kind": "dag_ctx", "field": "planned_for"}))]
fn references_serialize_with_a_kind_tag(
    #[case] reference: Reference,
    #[case] expected: serde_json::Value,
) {
    assert_eq!(serde_json::to_value(&reference).unwrap(), expected);
    assert_eq!(
        serde_json::from_value::<Reference>(expected).unwrap(),
        reference
    );
}

#[rstest]
#[case(DagCtxField::RunId, "run_id")]
#[case(DagCtxField::RunKind, "run_kind")]
#[case(DagCtxField::PlannedFor, "planned_for")]
#[case(DagCtxField::Since, "since")]
#[case(DagCtxField::DagId, "dag_id")]
fn dag_ctx_fields_serialize_in_snake_case(#[case] field: DagCtxField, #[case] name: &str) {
    assert_eq!(serde_json::to_value(field).unwrap(), json!(name));
}

#[test]
fn upstream_task_only_for_output_references() {
    assert_eq!(
        output("extract", vec![]).upstream_task(),
        Some(&task("extract"))
    );
    assert_eq!(planned_for().upstream_task(), None);
}

#[test]
fn templates_serialize_as_lists_of_parts() {
    let template = Template(vec![
        TemplatePart::Text("./notify.sh ".into()),
        TemplatePart::Ref(TemplateRef {
            reference: planned_for(),
            format: None,
        }),
        TemplatePart::Text(" ".into()),
        TemplatePart::Ref(TemplateRef {
            reference: output("count", vec![]),
            format: Some("raw".into()),
        }),
    ]);
    let expected = json!([
        {"text": "./notify.sh "},
        {"ref": {"kind": "dag_ctx", "field": "planned_for"}},
        {"text": " "},
        {"ref": {"kind": "output", "task": "count", "format": "raw"}},
    ]);
    assert_eq!(serde_json::to_value(&template).unwrap(), expected);
    assert_eq!(
        serde_json::from_value::<Template>(expected).unwrap(),
        template
    );
}

#[test]
fn template_text_and_references() {
    assert_eq!(
        Template::text("ls"),
        Template(vec![TemplatePart::Text("ls".into())])
    );
    let template = Template(vec![
        TemplatePart::Text("a".into()),
        TemplatePart::Ref(TemplateRef {
            reference: planned_for(),
            format: None,
        }),
        TemplatePart::Ref(TemplateRef {
            reference: output("x", vec![]),
            format: None,
        }),
    ]);
    assert_eq!(
        template.references().collect::<Vec<_>>(),
        vec![&planned_for(), &output("x", vec![])]
    );
    assert_eq!(Template::default().references().count(), 0);
}

#[test]
fn template_parts_reject_unknown_variants() {
    assert!(serde_json::from_value::<TemplatePart>(json!({"code": "x"})).is_err());
}

#[test]
fn values_serialize_externally_tagged() {
    let value = Value::Map(BTreeMap::from([
        ("day".to_owned(), Value::Ref(planned_for())),
        ("limit".to_owned(), Value::Literal(json!(10))),
        (
            "names".to_owned(),
            Value::List(vec![Value::Template(Template::text("a"))]),
        ),
    ]));
    let expected = json!({"map": {
        "day": {"ref": {"kind": "dag_ctx", "field": "planned_for"}},
        "limit": {"literal": 10},
        "names": {"list": [{"template": [{"text": "a"}]}]},
    }});
    assert_eq!(serde_json::to_value(&value).unwrap(), expected);
    assert_eq!(serde_json::from_value::<Value>(expected).unwrap(), value);
}

#[test]
fn literals_keep_tagged_extra_types_as_is() {
    let tagged = json!({"$datetime": "2026-10-19T03:00:00+00:00"});
    let value: Value = serde_json::from_value(json!({"literal": tagged.clone()})).unwrap();
    assert_eq!(value, Value::Literal(tagged));
}

#[test]
fn value_references_are_found_at_any_depth_in_order() {
    let value = Value::List(vec![
        Value::Literal(json!("x")),
        Value::Ref(output("a", vec![])),
        Value::Map(BTreeMap::from([(
            "k".to_owned(),
            Value::Template(Template(vec![TemplatePart::Ref(TemplateRef {
                reference: output("b", vec![PathElem::Index(1)]),
                format: None,
            })])),
        )])),
        Value::List(vec![Value::Ref(planned_for())]),
    ]);
    assert_eq!(
        value.references(),
        vec![
            &output("a", vec![]),
            &output("b", vec![PathElem::Index(1)]),
            &planned_for()
        ]
    );
    assert_eq!(
        Value::Literal(json!(1)).references(),
        Vec::<&Reference>::new()
    );
}

#[test]
fn values_reject_unknown_variants() {
    assert!(serde_json::from_value::<Value>(json!({"pickle": "x"})).is_err());
}
