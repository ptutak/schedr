use super::*;
use schedr_model::task::{CleanupRun, TaskCleanup};
use serde_json::json;

fn cleanup(handles: &[&str]) -> TaskCleanup {
    TaskCleanup {
        run: CleanupRun::Python,
        timeout_ms: Some(1000),
        retries: 0,
        retry_delay_ms: 0,
        handles: (!handles.is_empty()).then(|| handles.iter().map(ToString::to_string).collect()),
    }
}

fn handle(handle_type: &str, value: serde_json::Value) -> CleanupHandle {
    CleanupHandle {
        handle_type: handle_type.to_owned(),
        value,
    }
}

fn handles(taken: &[&str]) -> AttemptHandles {
    AttemptHandles::new(Some(&cleanup(taken)), Arc::new(MaskSet::new(4)))
}

#[test]
fn handles_are_kept_in_registration_order() {
    let handles = handles(&[]);
    handles.add(handle("B", json!({"n": 1}))).unwrap();
    handles.add(handle("A", json!({"n": 2}))).unwrap();
    let types: Vec<String> = handles.list().into_iter().map(|h| h.handle_type).collect();
    assert_eq!(types, ["B", "A"]);
}

#[test]
fn a_task_without_a_cleanup_takes_no_handle() {
    let handles = AttemptHandles::new(None, Arc::new(MaskSet::new(4)));
    assert_eq!(
        handles.add(handle("A", json!({}))),
        Err(HandleError::NoCleanup)
    );
}

#[test]
fn a_type_the_cleanup_does_not_take_is_refused() {
    let handles = handles(&["ExportJob", "TempDir"]);
    handles.add(handle("TempDir", json!({}))).unwrap();
    let error = handles.add(handle("Other", json!({}))).unwrap_err();
    assert_eq!(
        error.to_string(),
        "handle type `Other` isn't one its cleanup takes (ExportJob, TempDir)"
    );
}

#[test]
fn a_secret_in_a_handle_is_masked_before_it_is_kept() {
    let mask = Arc::new(MaskSet::new(4));
    mask.add("etl/db#password", "hunter22");
    let handles = AttemptHandles::new(Some(&cleanup(&[])), mask);
    handles
        .add(handle("Job", json!({"token": "hunter22", "id": "j-1"})))
        .unwrap();
    assert_eq!(
        handles.list()[0].value,
        json!({"token": "****", "id": "j-1"})
    );
}

#[test]
fn handles_over_the_limit_in_all_are_refused_and_the_rest_kept() {
    let handles = handles(&[]);
    let big = "x".repeat(MAX_HANDLES_SIZE / 2);
    handles.add(handle("A", json!(big))).unwrap();
    let error = handles.add(handle("A", json!(big))).unwrap_err();
    assert!(matches!(error, HandleError::TooLarge { .. }), "{error}");
    assert_eq!(handles.list().len(), 1);
    handles.add(handle("A", json!("small"))).unwrap();
}

#[test]
fn a_cleanup_registers_no_handle() {
    let handles = handles(&[]);
    handles.close();
    assert_eq!(
        handles.add(handle("A", json!({}))),
        Err(HandleError::InCleanup)
    );
}

#[test]
fn a_cleanup_taking_no_handles_refuses_them() {
    let cleanup = TaskCleanup {
        handles: Some(Vec::new()),
        ..cleanup(&[])
    };
    let handles = AttemptHandles::new(Some(&cleanup), Arc::new(MaskSet::new(4)));
    assert_eq!(
        handles.add(handle("A", json!({}))),
        Err(HandleError::NoneTaken)
    );
}

#[test]
fn all_that_was_accepted_can_be_read_back_in_one_message() {
    let handles = handles(&[]);
    let field = "x".repeat(1000);
    let mut accepted = 0;
    while handles
        .add(handle(
            "A",
            json!({"long_field_name": field, "n": accepted}),
        ))
        .is_ok()
    {
        accepted += 1;
    }
    assert!(accepted > 900, "{accepted}");
    let answer = schedr_model::ctx::CtxAnswer::Handles {
        handles: handles.list(),
    };
    let size = serde_json::to_vec(&answer).unwrap().len();
    assert!(
        size <= schedr_model::ctx::MAX_CTX_MESSAGE_SIZE as usize,
        "{size}"
    );
}
