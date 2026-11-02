use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn field(name: &str, kind: ParamKind, default: Option<Value>) -> ParamFieldView {
    ParamFieldView {
        name: name.to_owned(),
        kind,
        required: default.is_none(),
        nullable: false,
        secret: false,
        default,
        choices: Vec::new(),
    }
}

/// The posted form: `pairs`, with `params` the fingerprint of `fields`.
fn form(fields: &[ParamFieldView], pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut form: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    form.push(("params".to_owned(), fingerprint(fields)));
    form
}

#[test]
fn a_form_without_params_sends_none() {
    let fields = [field("rows", ParamKind::Int, Some(json!(1)))];
    let posted = [("for".to_owned(), String::new())];
    assert_eq!(values(&fields, &posted), Ok(None));
}

#[test]
fn a_box_and_a_text_are_sent_when_changed_even_to_nothing() {
    let fields = [
        field("full", ParamKind::Bool, Some(json!(true))),
        field("note", ParamKind::Text, Some(json!("x"))),
        field("name", ParamKind::Text, None),
        field("ratio", ParamKind::Float, Some(json!(0.5))),
    ];
    // The box unchecked sends nothing: false. The default's text emptied: the empty string. A
    // text with no default left empty: missing (the master says so).
    let sent = values(
        &fields,
        &form(
            &fields,
            &[("p:note", ""), ("p:name", ""), ("p:ratio", "0.5")],
        ),
    );
    assert_eq!(
        sent,
        Ok(Some(
            json!({"full": false, "note": ""})
                .as_object()
                .unwrap()
                .clone()
        ))
    );
}

#[test]
fn none_and_secrets_are_read_as_their_kind() {
    let mut day = field("day", ParamKind::Date, Some(json!("2026-10-15")));
    day.nullable = true;
    let mut port = field("db_port_secret", ParamKind::Int, Some(json!(1)));
    port.secret = true;
    port.default = None;
    port.required = false;
    let mut token = field("api_token", ParamKind::Text, None);
    token.secret = true;
    token.required = false;
    let mut key = field("key", ParamKind::Text, None);
    key.secret = true;
    let fields = [day, port, token, key];
    let sent = values(
        &fields,
        &form(
            &fields,
            &[
                ("p:day", "2026-10-15"),
                ("none:day", "on"),
                ("p:db_port_secret", "5432"),
                ("p:api_token", "123"),
                ("p:key", ""),
            ],
        ),
    );
    assert_eq!(
        sent,
        Ok(Some(
            json!({"day": null, "db_port_secret": 5432, "api_token": "123"})
                .as_object()
                .unwrap()
                .clone()
        ))
    );
}

#[test]
fn an_input_starts_from_its_default() {
    let mut mode = field("mode", ParamKind::Choice, None);
    mode.choices = vec![json!("fast"), json!(2)];
    let input = ParamInput::of(&mode);
    assert_eq!(
        input.options,
        [
            (String::new(), "choose".to_owned(), true),
            ("\"fast\"".to_owned(), "\"fast\"".to_owned(), false),
            ("2".to_owned(), "2".to_owned(), false),
        ]
    );
    assert!(input.required);
    let mut day = field("day", ParamKind::Date, None);
    day.nullable = true;
    // A required field with a "none" box: checking it answers too, so the browser doesn't ask.
    assert!(!ParamInput::of(&day).required);
    let full = ParamInput::of(&field("full", ParamKind::Bool, Some(json!(true))));
    assert_eq!((full.control, full.checked), ("checkbox", true));
}

#[test]
fn a_page_built_before_the_params_changed_is_refused() {
    // The page showed `region`; the DAG now takes `enabled` instead.
    let shown = [field("region", ParamKind::Text, Some(json!("eu")))];
    let now = [field("enabled", ParamKind::Bool, Some(json!(true)))];
    let posted = form(&shown, &[("p:region", "us")]);
    assert_eq!(
        values(&now, &posted),
        Err(
            "the DAG's parameters changed since this page was loaded: reload it and trigger \
             again"
                .to_owned()
        )
    );
    // A default changed meanwhile too (decision 159): the box left showing the old one would
    // send it as chosen.
    let before = [field("rows", ParamKind::Int, Some(json!(10)))];
    let after = [field("rows", ParamKind::Int, Some(json!(20)))];
    assert!(values(&after, &form(&before, &[("p:rows", "10")])).is_err());
    // A `null` default and none at all differ.
    let mut nothing = field("day", ParamKind::Date, None);
    nothing.nullable = true;
    let mut null = nothing.clone();
    null.default = Some(Value::Null);
    assert_ne!(fingerprint(&[nothing]), fingerprint(&[null]));
    // A secret's default isn't in the view: a page stays good when only it changes.
    let mut token = field("api_token", ParamKind::Text, None);
    token.secret = true;
    token.required = false;
    let shown = [token];
    assert_eq!(
        values(&shown, &form(&shown, &[("p:api_token", "")])),
        Ok(Some(Map::new()))
    );
}

#[test]
fn numbers_keep_their_type_and_width() {
    let mut code = field("access_token", ParamKind::Choice, None);
    code.secret = true;
    code.required = false;
    let rows = field("rows", ParamKind::Int, Some(json!(1)));
    let fields = [code, rows];
    let sent = values(
        &fields,
        &form(
            &fields,
            &[("p:access_token", "123"), ("p:rows", "9223372036854775808")],
        ),
    );
    assert_eq!(
        sent,
        Ok(Some(
            json!({"access_token": 123, "rows": 9_223_372_036_854_775_808_u64})
                .as_object()
                .unwrap()
                .clone()
        ))
    );
}

#[test]
fn mixed_choices_are_labelled_apart() {
    let mut mode = field("mode", ParamKind::Choice, Some(json!(1)));
    mode.choices = vec![json!("1"), json!(1), json!("true"), json!(true)];
    let labels: Vec<String> = ParamInput::of(&mode)
        .options
        .into_iter()
        .map(|(_, label, _)| label)
        .collect();
    assert_eq!(labels, ["\"1\"", "1", "\"true\"", "true"]);
}
