use pretty_assertions::assert_eq;
use rstest::rstest;
use schedr_model::{TaskId, TemplateRef};

use super::*;

fn ctx() -> RenderContext {
    let mut ctx = RenderContext::new(
        "sync",
        "0192c4e1",
        "manual",
        "2026-10-15T03:00:00Z",
        "2026-10-14T03:00:00Z",
    );
    ctx.inputs.insert("count".into(), json!(120));
    ctx.inputs.insert(
        "extract".into(),
        json!({"rows": [{"id": "a b"}, {"id": "c"}], "pair": {"$tuple": [1, "x"]}}),
    );
    ctx.inputs.insert(
        "listing".into(),
        json!({"stdout": "...tail", "stderr": "", "exit_code": 0,
               "stdout_truncated": true, "stderr_truncated": false}),
    );
    ctx
}

fn output(task: &str, path: Vec<PathElem>) -> Reference {
    Reference::Output {
        on: Vec::new(),
        task: TaskId::new(task).unwrap(),
        path,
    }
}

fn field(name: &str) -> PathElem {
    PathElem::Field(name.into())
}

fn part(reference: Reference, format: Option<&str>) -> TemplatePart {
    TemplatePart::Ref(TemplateRef {
        reference,
        format: format.map(str::to_owned),
    })
}

fn text(s: &str) -> TemplatePart {
    TemplatePart::Text(s.into())
}

const PLANNED_FOR: Reference = Reference::DagCtx {
    field: DagCtxField::PlannedFor,
};

#[rstest]
#[case(Reference::DagCtx { field: DagCtxField::DagId }, "dag_ctx.dag_id")]
#[case(Reference::DagCtx { field: DagCtxField::RunId }, "dag_ctx.run.id")]
#[case(Reference::DagCtx { field: DagCtxField::RunKind }, "dag_ctx.run.kind")]
#[case(PLANNED_FOR, "dag_ctx.run.planned_for")]
#[case(output("extract", vec![field("rows"), PathElem::Index(0), field("id")]), "extract.rows[0].id")]
fn reference_labels(#[case] reference: Reference, #[case] label: &str) {
    assert_eq!(reference_label(&reference), label);
}

#[test]
fn resolve_follows_paths_tuples_and_dag_ctx() {
    let ctx = ctx();
    assert_eq!(
        resolve(&output("count", vec![]), &ctx).unwrap(),
        &json!(120)
    );
    assert_eq!(
        resolve(
            &output(
                "extract",
                vec![field("rows"), PathElem::Index(1), field("id")]
            ),
            &ctx
        )
        .unwrap(),
        &json!("c")
    );
    assert_eq!(
        resolve(
            &output("extract", vec![field("pair"), PathElem::Index(1)]),
            &ctx
        )
        .unwrap(),
        &json!("x")
    );
    assert_eq!(
        resolve(&PLANNED_FOR, &ctx).unwrap(),
        &json!({"$datetime": "2026-10-15T03:00:00Z"})
    );
}

#[rstest]
#[case(output("missing", vec![]), RenderError::MissingInput("missing".into()))]
#[case(output("extract", vec![field("nope")]), RenderError::MissingPath("extract.nope".into()))]
#[case(output("extract", vec![field("rows"), PathElem::Index(9)]), RenderError::MissingPath("extract.rows[9]".into()))]
#[case(output("count", vec![field("x")]), RenderError::MissingPath("count.x".into()))]
#[case(output("count", vec![PathElem::Index(0)]), RenderError::MissingPath("count[0]".into()))]
#[case(output("listing", vec![field("stdout")]), RenderError::Truncated("listing.stdout".into()))]
fn resolve_errors(#[case] reference: Reference, #[case] expected: RenderError) {
    assert_eq!(resolve(&reference, &ctx()).unwrap_err(), expected);
}

#[test]
fn a_missing_dag_ctx_field_is_a_missing_input() {
    let err = resolve(&PLANNED_FOR, &RenderContext::default()).unwrap_err();
    assert_eq!(
        err,
        RenderError::MissingInput("dag_ctx.run.planned_for".into())
    );
}

#[test]
fn an_untruncated_stream_resolves() {
    let ctx = ctx();
    assert_eq!(
        resolve(&output("listing", vec![field("stderr")]), &ctx).unwrap(),
        &json!("")
    );
}

#[rstest]
#[case(0.0, "0.0")]
#[case(1.0, "1.0")]
#[case(1.5, "1.5")]
#[case(-2.25, "-2.25")]
#[case(0.1, "0.1")]
#[case(1e15, "1000000000000000.0")]
#[case(1e16, "1e+16")]
#[case(1.5e20, "1.5e+20")]
#[case(0.0001, "0.0001")]
#[case(0.00001, "1e-05")]
#[case(-0.0, "-0.0")]
#[case(f64::NAN, "nan")]
#[case(f64::INFINITY, "inf")]
#[case(f64::NEG_INFINITY, "-inf")]
fn floats_are_written_like_python(#[case] value: f64, #[case] expected: &str) {
    assert_eq!(python_float(value), expected);
}

#[rstest]
#[case(json!("a b"), None, "a b")]
#[case(json!(7), None, "7")]
#[case(json!(1.5), None, "1.5")]
#[case(json!(true), None, "True")]
#[case(json!(false), None, "False")]
#[case(json!({"$datetime": "2026-10-19T03:00:00+00:00"}), None, "2026-10-19T03:00:00+00:00")]
#[case(json!({"$datetime": "2026-10-19T05:30:00+02:00"}), Some("%Y-%m-%d %H:%M %z"), "2026-10-19 05:30 +0200")]
#[case(json!({"$datetime": "2026-10-15T03:00:00Z"}), Some("%d.%m.%Y"), "15.10.2026")]
#[case(json!({"$date": "2026-10-19"}), None, "2026-10-19")]
#[case(json!({"$date": "2026-10-19"}), Some("%Y%m%d"), "20261019")]
#[case(json!({"$timedelta": 3_600_000_000_i64}), None, "3600")]
#[case(json!({"$timedelta": 1_500_000}), None, "1.5")]
#[case(json!({"$timedelta": -86_400_000_000_i64}), None, "-86400")]
#[case(json!("x"), Some(""), "x")]
fn values_as_text(#[case] value: Value, #[case] spec: Option<&str>, #[case] expected: &str) {
    assert_eq!(to_text(&value, spec, "r").unwrap(), expected);
}

#[rstest]
#[case(json!(null), "`r` is None, which can't be rendered into text")]
#[case(json!([1]), "`r` is a list, which can't be rendered into text")]
#[case(json!({"a": 1}), "`r` is an object, which can't be rendered into text")]
fn unrenderable_values(#[case] value: Value, #[case] message: &str) {
    assert_eq!(to_text(&value, None, "r").unwrap_err().to_string(), message);
}

#[rstest]
#[case(json!(5), "%Y", "only dates and times take a format spec")]
#[case(json!("x"), "raw", "only dates and times take a format spec")]
#[case(json!({"$timedelta": 1}), "%S", "durations are written as seconds")]
#[case(json!({"$datetime": "not a date"}), "%Y", "the stored date is invalid")]
#[case(json!({"$datetime": "2026-10-19T03:00:00+00:00"}), "%Y %", "")]
fn bad_format_specs(#[case] value: Value, #[case] spec: &str, #[case] reason: &str) {
    let err = to_text(&value, Some(spec), "r").unwrap_err();
    assert!(matches!(&err, RenderError::BadFormat { .. }), "{err:?}");
    assert!(err.to_string().contains(reason), "{err}");
}

#[test]
fn render_writes_references_as_they_are() {
    let template = Template(vec![
        text("./notify.sh "),
        part(
            output(
                "extract",
                vec![field("rows"), PathElem::Index(0), field("id")],
            ),
            None,
        ),
        text(" "),
        part(output("count", vec![]), None),
        text(" "),
        part(PLANNED_FOR, Some("%Y-%m-%d")),
    ]);
    assert_eq!(
        render(&template, &ctx()).unwrap(),
        "./notify.sh a b 120 2026-10-15"
    );
}

#[test]
fn render_reports_resolution_errors() {
    let template = Template(vec![part(output("missing", vec![]), None)]);
    assert_eq!(
        render(&template, &ctx()).unwrap_err().to_string(),
        "the output of `missing` isn't among the task's inputs"
    );
}

#[test]
fn shell_command_lines_run_through_sh() {
    let line = CommandLine::Shell(Template::text("echo \"$URL\""));
    assert_eq!(
        command_line(&line, &ctx()).unwrap(),
        (
            "/bin/sh".to_owned(),
            vec!["-c".to_owned(), "echo \"$URL\"".to_owned()]
        )
    );
}

/// Decision 99: a value never becomes shell source, whatever DAG JSON the agent is sent.
#[test]
fn a_shell_command_with_a_reference_is_refused_before_launch() {
    let line = CommandLine::Shell(Template(vec![
        text("echo "),
        part(output("count", vec![]), None),
    ]));
    assert_eq!(
        command_line(&line, &ctx()).unwrap_err(),
        RenderError::ShellPlaceholder("count".to_owned())
    );
}

#[test]
fn argument_lists_render_each_element_unquoted() {
    let line = CommandLine::Argv(vec![
        Template::text("./notify.sh"),
        Template(vec![text("--day="), part(PLANNED_FOR, Some("%Y-%m-%d"))]),
        Template(vec![part(
            output(
                "extract",
                vec![field("rows"), PathElem::Index(0), field("id")],
            ),
            None,
        )]),
    ]);
    assert_eq!(
        command_line(&line, &ctx()).unwrap(),
        (
            "./notify.sh".to_owned(),
            vec!["--day=2026-10-15".to_owned(), "a b".to_owned()]
        )
    );
}

#[test]
fn an_empty_argument_list_has_no_program() {
    assert_eq!(
        command_line(&CommandLine::Argv(vec![]), &ctx()).unwrap(),
        (String::new(), vec![])
    );
}

#[test]
fn argument_list_errors_are_reported() {
    let line = CommandLine::Argv(vec![Template(vec![part(output("missing", vec![]), None)])]);
    assert!(command_line(&line, &ctx()).is_err());
}

fn narrowed(task: &str, on: &[&str]) -> Reference {
    Reference::Output {
        task: TaskId::new(task).unwrap(),
        path: vec![field("rows")],
        on: on.iter().map(|o| (*o).to_owned()).collect(),
    }
}

#[test]
fn a_reference_whose_edge_wasnt_taken_stands_for_null() {
    let mut ctx = ctx();
    ctx.outcomes.insert("extract".into(), json!("Rows"));
    // Taken: the value.
    assert_eq!(
        resolve(&narrowed("extract", &["Rows"]), &ctx).unwrap(),
        &json!([{"id": "a b"}, {"id": "c"}])
    );
    assert!(resolve(&narrowed("extract", &["Unavailable", "Rows"]), &ctx).is_ok());
    // Another outcome: null, without reading the output.
    assert_eq!(
        resolve(&narrowed("extract", &["Unavailable"]), &ctx).unwrap(),
        &Value::Null
    );
    // A skipped upstream: null for any reference to it, narrowed or not.
    ctx.outcomes.insert("gone".into(), Value::Null);
    assert_eq!(
        resolve(&output("gone", vec![]), &ctx).unwrap(),
        &Value::Null
    );
    assert_eq!(
        resolve(&narrowed("gone", &["Rows"]), &ctx).unwrap(),
        &Value::Null
    );
    // Neither outcome nor output known: still a missing input.
    assert_eq!(
        resolve(&output("unknown", vec![]), &ctx).unwrap_err(),
        RenderError::MissingInput("unknown".into())
    );
}

#[test]
fn an_outcome_reference_is_the_outcomes_name() {
    let mut ctx = ctx();
    ctx.outcomes.insert("check".into(), json!("CHANGED"));
    let outcome = |task: &str| Reference::Outcome {
        task: TaskId::new(task).unwrap(),
    };
    assert_eq!(resolve(&outcome("check"), &ctx).unwrap(), &json!("CHANGED"));
    assert_eq!(reference_label(&outcome("check")), "check.outcome");
    assert_eq!(
        resolve(&outcome("other"), &ctx).unwrap_err(),
        RenderError::MissingInput("other.outcome".into())
    );
}
