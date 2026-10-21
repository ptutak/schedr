use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

#[rstest]
#[case(Termination::Exited(0), None, AttemptStatus::Success)]
#[case(Termination::Exited(1), None, AttemptStatus::Failed)]
#[case(Termination::Signaled(11), None, AttemptStatus::Failed)]
#[case(
    Termination::Exited(0),
    Some(StopReason::TimedOut),
    AttemptStatus::TimedOut
)]
#[case(
    Termination::Signaled(15),
    Some(StopReason::TimedOut),
    AttemptStatus::TimedOut
)]
#[case(
    Termination::Signaled(9),
    Some(StopReason::Cancelled),
    AttemptStatus::Cancelled
)]
#[case(
    Termination::Exited(0),
    Some(StopReason::Interrupted),
    AttemptStatus::Interrupted
)]
#[case(
    Termination::Signaled(9),
    Some(StopReason::OomKilled),
    AttemptStatus::OomKilled
)]
fn status_from_termination_and_stop_reason(
    #[case] termination: Termination,
    #[case] stopped: Option<StopReason>,
    #[case] expected: AttemptStatus,
) {
    assert_eq!(attempt_status(termination, stopped, &[0]), expected);
}

#[test]
fn declared_outcome_codes_are_successes() {
    assert_eq!(
        attempt_status(Termination::Exited(3), None, &[0, 3]),
        AttemptStatus::Success
    );
    assert_eq!(
        attempt_status(Termination::Exited(0), None, &[1, 3]),
        AttemptStatus::Failed
    );
}

#[test]
fn exit_codes() {
    assert_eq!(Termination::Exited(4).exit_code(), 4);
    assert_eq!(Termination::Signaled(9).exit_code(), 137);
}
