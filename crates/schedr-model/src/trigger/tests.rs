use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::json;

use super::*;

fn cron() -> Trigger {
    Trigger::Cron {
        expression: "0 3 * * *".into(),
        timezone: "Europe/Warsaw".into(),
        catchup: false,
        start: Some(ts("2026-01-01T00:00:00Z")),
        end: None,
    }
}

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn every(catchup: bool) -> Trigger {
    Trigger::Every {
        interval_ms: 3_600_000,
        catchup,
        start: None,
        end: Some(ts("2026-12-31T00:00:00Z")),
    }
}

fn once() -> Trigger {
    Trigger::Once {
        at: "2026-12-24T22:00:00Z".parse().unwrap(),
    }
}

#[rstest]
#[case(Trigger::None, json!({"kind": "none"}))]
#[case(cron(), json!({"kind": "cron", "expression": "0 3 * * *", "timezone": "Europe/Warsaw", "catchup": false, "start": "2026-01-01T00:00:00Z", "end": null}))]
#[case(every(true), json!({"kind": "every", "interval_ms": 3_600_000, "catchup": true, "start": null, "end": "2026-12-31T00:00:00Z"}))]
#[case(once(), json!({"kind": "once", "at": "2026-12-24T22:00:00Z"}))]
fn serializes_with_a_kind_tag(#[case] trigger: Trigger, #[case] expected: serde_json::Value) {
    assert_eq!(serde_json::to_value(&trigger).unwrap(), expected);
    assert_eq!(
        serde_json::from_value::<Trigger>(expected).unwrap(),
        trigger
    );
}

#[test]
fn rejects_unknown_fields() {
    let err = serde_json::from_value::<Trigger>(
        json!({"kind": "every", "interval_ms": 1, "catchup": false, "x": 1}),
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown field `x`"), "{err}");
}

#[test]
fn rejects_catchup_on_once() {
    let err = serde_json::from_value::<Trigger>(
        json!({"kind": "once", "at": "2026-12-24T22:00:00Z", "catchup": true}),
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown field `catchup`"), "{err}");
}

#[test]
fn rejects_unknown_kinds() {
    assert!(serde_json::from_value::<Trigger>(json!({"kind": "sometimes"})).is_err());
}

#[rstest]
#[case(Trigger::None, true)]
#[case(cron(), false)]
#[case(every(false), false)]
#[case(once(), false)]
fn is_manual_only_only_without_a_trigger(#[case] trigger: Trigger, #[case] expected: bool) {
    assert_eq!(trigger.is_manual_only(), expected);
}

#[rstest]
#[case(Trigger::None, false)]
#[case(cron(), false)]
#[case(Trigger::Cron { expression: "@daily".into(), timezone: "UTC".into(), catchup: true, start: None, end: None }, true)]
#[case(every(true), true)]
#[case(every(false), false)]
#[case(once(), false)]
fn catchup_comes_from_time_triggers(#[case] trigger: Trigger, #[case] expected: bool) {
    assert_eq!(trigger.catchup(), expected);
}

#[test]
fn a_missing_start_or_end_is_none() {
    let trigger: Trigger =
        serde_json::from_value(json!({"kind": "every", "interval_ms": 1, "catchup": false}))
            .unwrap();
    assert_eq!((trigger.start(), trigger.end()), (None, None));
}

#[test]
fn rejects_a_window_on_once() {
    let err = serde_json::from_value::<Trigger>(
        json!({"kind": "once", "at": "2026-12-24T22:00:00Z", "start": null}),
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown field `start`"), "{err}");
}

#[rstest]
#[case(Trigger::None, None, None)]
#[case(once(), None, None)]
#[case(cron(), Some(ts("2026-01-01T00:00:00Z")), None)]
#[case(every(false), None, Some(ts("2026-12-31T00:00:00Z")))]
fn the_window_comes_from_time_triggers(
    #[case] trigger: Trigger,
    #[case] start: Option<Timestamp>,
    #[case] end: Option<Timestamp>,
) {
    assert_eq!((trigger.start(), trigger.end()), (start, end));
}
