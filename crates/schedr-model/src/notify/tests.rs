use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn reference(name: Placeholder, path: Vec<PathKey>) -> Ref {
    Ref { name, path }
}

#[test]
fn a_config_string_of_one_placeholder_keeps_the_value_s_type() {
    assert_eq!(
        Body::from_config(&json!("{failed_tasks}")),
        Ok(Body::Ref(reference(Placeholder::FailedTasks, vec![])))
    );
}

#[test]
fn a_config_string_mixes_text_paths_and_literal_braces() {
    let body = Body::from_config(&json!("{{ops}} {dag}: {data[rows]} rows, {data[items][0]}"));
    assert_eq!(
        body,
        Ok(Body::Template(vec![
            Part::Text("{ops} ".into()),
            Part::Ref(reference(Placeholder::Dag, vec![])),
            Part::Text(": ".into()),
            Part::Ref(reference(
                Placeholder::Data,
                vec![PathKey::Key("rows".into())]
            )),
            Part::Text(" rows, ".into()),
            Part::Ref(reference(
                Placeholder::Data,
                vec![PathKey::Key("items".into()), PathKey::Index(0)]
            )),
        ]))
    );
}

#[test]
fn a_config_body_walks_objects_and_arrays_and_keeps_other_values() {
    let body = Body::from_config(&json!({"a": ["x", 1, null], "b": {"c": "{run}"}})).unwrap();
    assert_eq!(
        body,
        Body::Object(
            [
                (
                    "a".to_owned(),
                    Body::Array(vec![
                        Body::Value(json!("x")),
                        Body::Value(json!(1)),
                        Body::Value(json!(null)),
                    ])
                ),
                (
                    "b".to_owned(),
                    Body::Object(
                        [(
                            "c".to_owned(),
                            Body::Ref(reference(Placeholder::Run, vec![]))
                        )]
                        .into()
                    )
                ),
            ]
            .into()
        )
    );
    assert_eq!(body.refs(), vec![&reference(Placeholder::Run, vec![])]);
}

#[test]
fn a_config_string_with_a_mistake_is_refused_naming_it() {
    for (text, says) in [
        ("{dgа}", "unknown placeholder `{dgа}`"),
        ("{dag", "isn't closed"),
        ("a } b", "a lone `}`"),
        ("{data[rows}", "a path is `[key]`"),
    ] {
        let error = Body::from_config(&json!(text)).unwrap_err();
        assert!(error.contains(says), "{text}: {error}");
    }
}

#[test]
fn a_body_round_trips_through_json() {
    let body = Body::Object(
        [(
            "title".to_owned(),
            Body::Template(vec![
                Part::Text("[x] ".into()),
                Part::Ref(reference(Placeholder::Event, vec![])),
            ]),
        )]
        .into(),
    );
    let text = serde_json::to_value(&body).unwrap();
    assert_eq!(
        text,
        json!({"object": {"title": {"template": [{"text": "[x] "}, {"ref": {"name": "event"}}]}}})
    );
    assert_eq!(serde_json::from_value::<Body>(text).unwrap(), body);
}

#[test]
fn a_notify_entry_names_a_server_channel_or_defines_a_webhook() {
    let entries: Vec<NotifyEntry> = serde_json::from_value(json!([
        {"channel": {"kind": "server", "name": "ops-slack"}},
        {"channel": {"kind": "webhook", "name": "relay", "url": "https://relay/x"},
         "events": ["RUN_FAILED", "TASK_RETRY"]},
    ]))
    .unwrap();
    assert_eq!(entries[0].channel.name(), "ops-slack");
    assert_eq!(entries[0].events, None);
    assert_eq!(entries[1].channel.name(), "relay");
    assert_eq!(
        entries[1].events,
        Some(vec![NotifyEvent::RunFailed, NotifyEvent::TaskRetry])
    );
}

#[test]
fn every_event_and_placeholder_parses_back_from_its_name() {
    for event in NotifyEvent::ALL {
        assert_eq!(NotifyEvent::parse(event.name()), Some(event));
        assert_eq!(serde_json::to_value(event).unwrap(), json!(event.name()));
    }
    for placeholder in Placeholder::ALL {
        assert_eq!(Placeholder::parse(placeholder.name()), Some(placeholder));
        assert_eq!(
            serde_json::to_value(placeholder).unwrap(),
            json!(placeholder.name())
        );
    }
}
