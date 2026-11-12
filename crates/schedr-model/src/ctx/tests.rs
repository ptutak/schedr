use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

#[test]
fn requests_are_tagged_by_type() {
    let hello = CtxRequest::Hello {
        protocol: CTX_PROTOCOL,
        schedr_version: "1.0.0".into(),
    };
    assert_eq!(
        serde_json::to_value(&hello).unwrap(),
        json!({"type": "hello", "protocol": 1, "schedr_version": "1.0.0"})
    );
    assert_eq!(
        serde_json::from_value::<CtxRequest>(json!({"type": "params_get"})).unwrap(),
        CtxRequest::ParamsGet
    );
    assert!(serde_json::from_value::<CtxRequest>(json!({"type": "secret_get"})).is_err());
}

#[test]
fn the_run_answer_is_flat() {
    let answer = CtxAnswer::Run(CtxRun {
        dag_id: "report".into(),
        id: "r1".into(),
        kind: RunKind::Manual,
        planned_for: "2026-11-01T08:00:00Z".parse().unwrap(),
        since: "2026-10-31T08:00:00Z".parse().unwrap(),
    });
    let expected = json!({
        "type": "run", "dag_id": "report", "id": "r1", "kind": "manual",
        "planned_for": "2026-11-01T08:00:00Z", "since": "2026-10-31T08:00:00Z",
    });
    assert_eq!(serde_json::to_value(&answer).unwrap(), expected);
    assert_eq!(
        serde_json::from_value::<CtxAnswer>(expected).unwrap(),
        answer
    );
}

#[test]
fn fields_added_within_the_protocol_are_ignored() {
    // A newer caller's hello and a newer agent's run, read by the older side.
    let hello = json!({"type": "hello", "protocol": 1, "schedr_version": "9", "extra": true});
    assert!(matches!(
        serde_json::from_value::<CtxRequest>(hello).unwrap(),
        CtxRequest::Hello { protocol: 1, .. }
    ));
    let run = json!({
        "type": "run", "dag_id": "d", "id": "r", "kind": "manual",
        "planned_for": "2026-11-01T08:00:00Z", "since": "2026-10-31T08:00:00Z",
        "later_field": 1,
    });
    assert!(matches!(
        serde_json::from_value::<CtxAnswer>(run).unwrap(),
        CtxAnswer::Run(_)
    ));
}
