use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::{Value as Json, json};

use super::*;
use TaskInstanceState as S;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn id(text: &str) -> TaskId {
    TaskId::new(text).unwrap()
}

fn task(name: &str, kind: &str) -> Json {
    json!({
        "id": name, "kind": kind,
        "settings": {
            "retries": 2, "retry_delay_ms": 60_000, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        },
        "output_type": {"type": "none"}
    })
}

/// `extract` → `load` → `report`, with `start` (empty) before `extract` and `audit` beside
/// `load` (also after `extract`); `report` joins `load` and `audit`.
fn dag() -> Dag {
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "none"}, "start": null, "end": null, "initial_since": null,
        "max_active_runs": 1,
        "tasks": [
            task("start", "empty"), task("extract", "python"), task("load", "python"),
            task("audit", "python"), task("report", "python")
        ],
        "edges": [
            {"from": "start", "to": "extract", "kind": "order"},
            {"from": "extract", "to": "load", "kind": "order"},
            {"from": "extract", "to": "audit", "kind": "order"},
            {"from": "load", "to": "report", "kind": "order"},
            {"from": "audit", "to": "report", "kind": "order"}
        ]
    }))
    .unwrap()
}

fn settings() -> TaskSettings {
    TaskSettings {
        retries: 2,
        retry_delay_ms: 60_000,
        ..TaskSettings::default()
    }
}

#[rstest]
#[case(&[], Readiness::Ready)]
#[case(&[S::Success, S::Success], Readiness::Ready)]
#[case(&[S::Success, S::Skipped], Readiness::Ready)]
#[case(&[S::Skipped, S::Skipped], Readiness::Skip)]
#[case(&[S::Success, S::Running], Readiness::Wait)]
#[case(&[S::UpForRetry], Readiness::Wait)]
#[case(&[S::Running, S::Failed], Readiness::UpstreamFailed)]
#[case(&[S::UpstreamFailed, S::Skipped], Readiness::UpstreamFailed)]
#[case(&[S::Cancelled, S::Success], Readiness::Cancel)]
fn none_failed_decides_as_soon_as_it_is_certain(
    #[case] upstream: &[TaskInstanceState],
    #[case] expected: Readiness,
) {
    assert_eq!(readiness(upstream), expected);
}

#[test]
fn successes_need_no_retry_and_cancels_are_never_retried() {
    let now = ts("2026-10-21T08:00:00Z");
    let s = settings();
    assert_eq!(
        after_attempt(AttemptStatus::Success, None, 0, &s, false, now),
        AfterAttempt::Succeeded
    );
    assert_eq!(
        after_attempt(AttemptStatus::Cancelled, None, 0, &s, false, now),
        AfterAttempt::Cancelled
    );
}

#[rstest]
#[case(AttemptStatus::Failed)]
#[case(AttemptStatus::TimedOut)]
#[case(AttemptStatus::OomKilled)]
#[case(AttemptStatus::Lost)]
fn failures_use_retries_after_the_delay(#[case] status: AttemptStatus) {
    let now = ts("2026-10-21T08:00:00Z");
    let s = settings();
    let retry = after_attempt(status, None, 1, &s, false, now);
    assert_eq!(retry, AfterAttempt::RetryAt(ts("2026-10-21T08:01:00Z")));
    assert!(retry.uses_retry());
    let last = after_attempt(status, None, 2, &s, false, now);
    assert_eq!(last, AfterAttempt::Failed);
    assert!(last.uses_retry());
}

#[rstest]
#[case(AttemptStatus::Interrupted)]
#[case(AttemptStatus::AssignmentExpired)]
fn attempts_that_werent_the_tasks_fault_run_again_without_a_retry(#[case] status: AttemptStatus) {
    let now = ts("2026-10-21T08:00:00Z");
    let again = after_attempt(status, None, 2, &settings(), false, now);
    assert_eq!(again, AfterAttempt::Requeue);
    assert!(!again.uses_retry());
    assert_eq!(
        after_attempt(status, None, 0, &settings(), true, now),
        AfterAttempt::Cancelled
    );
}

#[test]
fn failures_certain_to_repeat_and_failures_while_cancelling_arent_retried() {
    let now = ts("2026-10-21T08:00:00Z");
    for kind in NOT_RETRIED_ERRORS {
        assert_eq!(
            after_attempt(
                AttemptStatus::Failed,
                Some(kind),
                0,
                &settings(),
                false,
                now
            ),
            AfterAttempt::Failed,
            "{kind}"
        );
    }
    assert!(matches!(
        after_attempt(
            AttemptStatus::Failed,
            Some("import_failed"),
            0,
            &settings(),
            false,
            now
        ),
        AfterAttempt::RetryAt(_)
    ));
    assert_eq!(
        after_attempt(AttemptStatus::Failed, None, 0, &settings(), true, now),
        AfterAttempt::Failed
    );
}

#[test]
fn a_huge_retry_delay_saturates() {
    let s = TaskSettings {
        retries: 1,
        retry_delay_ms: u64::MAX,
        ..TaskSettings::default()
    };
    assert_eq!(
        after_attempt(
            AttemptStatus::Failed,
            None,
            0,
            &s,
            false,
            ts("2026-10-21T08:00:00Z")
        ),
        AfterAttempt::RetryAt(Timestamp::MAX)
    );
}

#[rstest]
#[case(&[S::Success, S::Skipped], Some(RunState::Success))]
#[case(&[S::Success, S::Failed, S::UpstreamFailed], Some(RunState::Failed))]
#[case(&[S::Success, S::Cancelled], Some(RunState::Cancelled))]
#[case(&[S::Failed, S::Cancelled], Some(RunState::Failed))]
#[case(&[S::Success, S::UpstreamFailed], Some(RunState::Success))]
#[case(&[S::Success, S::Running], None)]
#[case(&[], Some(RunState::Success))]
fn the_run_result_follows_the_rule(
    #[case] states: &[TaskInstanceState],
    #[case] expected: Option<RunState>,
) {
    assert_eq!(run_result(states), expected);
}

fn changes(list: &[(&str, TaskInstanceState)]) -> Vec<(TaskId, TaskInstanceState)> {
    list.iter().map(|(t, s)| (id(t), *s)).collect()
}

#[test]
fn a_new_run_completes_empty_tasks_and_queues_what_is_ready() {
    let dag = dag();
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    assert_eq!(
        run.settle(),
        changes(&[("start", S::Success), ("extract", S::Queued)])
    );
    assert_eq!(run.settle(), []);
    assert_eq!(run.result(), None);
}

#[test]
fn a_success_queues_the_downstream_tasks_and_a_join_waits_for_all() {
    let dag = dag();
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    run.settle();
    run.set(&id("extract"), S::Success);
    assert_eq!(
        run.settle(),
        changes(&[("load", S::Queued), ("audit", S::Queued)])
    );
    run.set(&id("load"), S::Success);
    assert_eq!(run.settle(), []);
    run.set(&id("audit"), S::Success);
    assert_eq!(run.settle(), changes(&[("report", S::Queued)]));
    run.set(&id("report"), S::Success);
    assert_eq!(run.result(), Some(RunState::Success));
}

#[test]
fn a_failure_fails_the_downstream_tasks_at_once_and_the_rest_keeps_running() {
    let dag = dag();
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    run.settle();
    run.set(&id("extract"), S::Success);
    run.settle();
    run.set(&id("load"), S::Failed);
    assert_eq!(run.settle(), changes(&[("report", S::UpstreamFailed)]));
    assert_eq!(run.states()[&id("audit")], S::Queued);
    assert_eq!(run.result(), None);
    run.set(&id("audit"), S::Success);
    assert_eq!(run.result(), Some(RunState::Failed));
}

#[test]
fn skips_propagate_when_every_upstream_task_was_skipped() {
    let dag = dag();
    let states = BTreeMap::from([
        (id("start"), S::Success),
        (id("extract"), S::Success),
        (id("load"), S::Skipped),
        (id("audit"), S::Skipped),
    ]);
    let mut run = RunProgress::new(&dag, states);
    assert_eq!(run.settle(), changes(&[("report", S::Skipped)]));
    assert_eq!(run.result(), Some(RunState::Success));
}

#[test]
fn cancelling_cancels_what_hasnt_started_and_reports_what_runs() {
    let dag = dag();
    let states = BTreeMap::from([
        (id("start"), S::Success),
        (id("extract"), S::Success),
        (id("load"), S::Running),
        (id("audit"), S::UpForRetry),
    ]);
    let mut run = RunProgress::new(&dag, states);
    let (cancelled, running) = run.cancel();
    assert_eq!(
        cancelled,
        changes(&[("audit", S::Cancelled), ("report", S::Cancelled)])
    );
    assert_eq!(running, [id("load")]);
    assert_eq!(run.result(), None);
    run.set(&id("load"), S::Success);
    assert_eq!(run.result(), Some(RunState::Cancelled));
}

#[test]
fn a_cancelled_upstream_task_cancels_its_downstream_tasks() {
    let dag = dag();
    let states = BTreeMap::from([(id("start"), S::Success), (id("extract"), S::Cancelled)]);
    let mut run = RunProgress::new(&dag, states);
    assert_eq!(
        run.settle(),
        changes(&[
            ("load", S::Cancelled),
            ("audit", S::Cancelled),
            ("report", S::Cancelled)
        ])
    );
}
