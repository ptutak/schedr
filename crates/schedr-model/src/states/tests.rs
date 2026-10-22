use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::json;

use super::*;

#[rstest]
#[case(RunKind::Scheduled, "scheduled")]
#[case(RunKind::Manual, "manual")]
#[case(RunKind::Backfill, "backfill")]
#[case(RunKind::Chained, "chained")]
#[case(RunKind::Event, "event")]
fn run_kinds_serialize_in_snake_case(#[case] kind: RunKind, #[case] name: &str) {
    assert_eq!(serde_json::to_value(kind).unwrap(), json!(name));
    assert_eq!(
        serde_json::from_value::<RunKind>(json!(name)).unwrap(),
        kind
    );
    assert_eq!(kind.to_string(), name);
}

#[rstest]
#[case(RunState::Queued, "queued", false)]
#[case(RunState::Running, "running", false)]
#[case(RunState::Success, "success", true)]
#[case(RunState::Failed, "failed", true)]
#[case(RunState::Cancelled, "cancelled", true)]
fn run_states(#[case] state: RunState, #[case] name: &str, #[case] is_final: bool) {
    assert_eq!(serde_json::to_value(state).unwrap(), json!(name));
    assert_eq!(
        serde_json::from_value::<RunState>(json!(name)).unwrap(),
        state
    );
    assert_eq!(state.to_string(), name);
    assert_eq!(state.is_final(), is_final);
}

#[rstest]
#[case(TaskInstanceState::Scheduled, "scheduled", false)]
#[case(TaskInstanceState::Queued, "queued", false)]
#[case(TaskInstanceState::Unschedulable, "unschedulable", false)]
#[case(TaskInstanceState::Running, "running", false)]
#[case(TaskInstanceState::CleaningUp, "cleaning_up", false)]
#[case(TaskInstanceState::UpForRetry, "up_for_retry", false)]
#[case(TaskInstanceState::Success, "success", true)]
#[case(TaskInstanceState::Failed, "failed", true)]
#[case(TaskInstanceState::Cancelled, "cancelled", true)]
#[case(TaskInstanceState::Skipped, "skipped", true)]
#[case(TaskInstanceState::UpstreamFailed, "upstream_failed", true)]
fn task_instance_states(
    #[case] state: TaskInstanceState,
    #[case] name: &str,
    #[case] is_final: bool,
) {
    assert_eq!(serde_json::to_value(state).unwrap(), json!(name));
    assert_eq!(
        serde_json::from_value::<TaskInstanceState>(json!(name)).unwrap(),
        state
    );
    assert_eq!(state.to_string(), name);
    assert_eq!(state.is_final(), is_final);
}

#[rstest]
#[case(AttemptStatus::Success, "success", RetryRule::NotNeeded)]
#[case(AttemptStatus::Failed, "failed", RetryRule::UsesRetry)]
#[case(AttemptStatus::TimedOut, "timed_out", RetryRule::UsesRetry)]
#[case(AttemptStatus::OomKilled, "oom_killed", RetryRule::UsesRetry)]
#[case(AttemptStatus::Lost, "lost", RetryRule::UsesRetry)]
#[case(
    AttemptStatus::Interrupted,
    "interrupted",
    RetryRule::RequeueWithoutRetry
)]
#[case(
    AttemptStatus::AssignmentExpired,
    "assignment_expired",
    RetryRule::RequeueWithoutRetry
)]
#[case(AttemptStatus::Cancelled, "cancelled", RetryRule::Never)]
fn attempt_statuses(#[case] status: AttemptStatus, #[case] name: &str, #[case] rule: RetryRule) {
    assert_eq!(serde_json::to_value(status).unwrap(), json!(name));
    assert_eq!(
        serde_json::from_value::<AttemptStatus>(json!(name)).unwrap(),
        status
    );
    assert_eq!(status.to_string(), name);
    assert_eq!(status.retry_rule(), rule);
}

#[test]
fn unknown_state_names_are_rejected() {
    assert!(serde_json::from_value::<RunState>(json!("paused")).is_err());
}

#[rstest]
#[case(WorkerState::Active, "active", false)]
#[case(WorkerState::Draining, "draining", false)]
#[case(WorkerState::Disconnected, "disconnected", false)]
#[case(WorkerState::Offline, "offline", true)]
#[case(WorkerState::Lost, "lost", true)]
fn worker_states_have_their_schema_names(
    #[case] state: WorkerState,
    #[case] name: &str,
    #[case] gone: bool,
) {
    assert_eq!(state.to_string(), name);
    assert_eq!(serde_json::to_value(state).unwrap(), json!(name));
    assert_eq!(state.is_gone(), gone);
}
