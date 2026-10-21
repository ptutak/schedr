use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::json;

use super::*;

fn cron() -> Schedule {
    Schedule::Cron {
        expression: "0 3 * * *".into(),
        timezone: "Europe/Warsaw".into(),
    }
}

#[rstest]
#[case(Schedule::None, json!({"kind": "none"}))]
#[case(cron(), json!({"kind": "cron", "expression": "0 3 * * *", "timezone": "Europe/Warsaw"}))]
#[case(Schedule::Every { interval_ms: 3_600_000 }, json!({"kind": "every", "interval_ms": 3_600_000}))]
#[case(Schedule::Once { at: "2026-12-24T22:00:00Z".parse().unwrap() }, json!({"kind": "once", "at": "2026-12-24T22:00:00Z"}))]
fn serializes_with_a_kind_tag(#[case] schedule: Schedule, #[case] expected: serde_json::Value) {
    assert_eq!(serde_json::to_value(&schedule).unwrap(), expected);
    assert_eq!(
        serde_json::from_value::<Schedule>(expected).unwrap(),
        schedule
    );
}

#[test]
fn rejects_unknown_fields() {
    let err =
        serde_json::from_value::<Schedule>(json!({"kind": "every", "interval_ms": 1, "x": 1}))
            .unwrap_err();
    assert!(err.to_string().contains("unknown field `x`"), "{err}");
}

#[test]
fn rejects_unknown_kinds() {
    assert!(serde_json::from_value::<Schedule>(json!({"kind": "sometimes"})).is_err());
}

#[rstest]
#[case(Schedule::None, true)]
#[case(cron(), false)]
#[case(Schedule::Every { interval_ms: 1 }, false)]
fn is_manual_only_only_without_a_schedule(#[case] schedule: Schedule, #[case] expected: bool) {
    assert_eq!(schedule.is_manual_only(), expected);
}
