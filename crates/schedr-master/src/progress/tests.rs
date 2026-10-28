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
        "trigger": {"kind": "none"}, "initial_since": null,
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
    assert_eq!(readiness(upstream, RunIf::NoneFailed), expected);
}

#[rstest]
#[case(&[], Readiness::Ready)]
#[case(&[S::Success, S::Success], Readiness::Ready)]
// One skip: skipped, where NoneFailed would run.
#[case(&[S::Success, S::Skipped], Readiness::Skip)]
#[case(&[S::Skipped, S::Skipped], Readiness::Skip)]
// A skip isn't certain while another edge could still fail (a failure wins).
#[case(&[S::Skipped, S::Running], Readiness::Wait)]
#[case(&[S::Skipped, S::Failed], Readiness::UpstreamFailed)]
#[case(&[S::Running, S::UpstreamFailed], Readiness::UpstreamFailed)]
#[case(&[S::Cancelled, S::Success], Readiness::Cancel)]
fn all_succeeded_runs_only_when_every_edge_succeeded(
    #[case] upstream: &[TaskInstanceState],
    #[case] expected: Readiness,
) {
    assert_eq!(readiness(upstream, RunIf::AllSucceeded), expected);
}

/// `fetch` (`-> Rows | Unavailable`) feeding `load` on `Rows`, `report_outage` on
/// `Unavailable`, and `both` on both (two edges from one task); `both_strict` the same
/// under `AllSucceeded`.
fn routed() -> Dag {
    let mut fetch = task("fetch", "python");
    fetch["outcomes"] = json!([{"name": "Rows"}, {"name": "Unavailable"}]);
    let mut both_strict = task("both_strict", "empty");
    both_strict["settings"]["run_if"] = json!("all_succeeded");
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "routed", "location": {"root": "etl", "path": "routed.py"},
        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
        "tasks": [
            fetch, task("load", "empty"), task("report_outage", "empty"),
            task("both", "empty"), both_strict
        ],
        "edges": [
            {"from": "fetch", "to": "load", "kind": "data", "on": ["Rows"]},
            {"from": "fetch", "to": "report_outage", "kind": "data", "on": ["Unavailable"]},
            {"from": "fetch", "to": "both", "kind": "data", "on": ["Rows"]},
            {"from": "fetch", "to": "both", "kind": "data", "on": ["Unavailable"]},
            {"from": "fetch", "to": "both_strict", "kind": "data", "on": ["Rows"]},
            {"from": "fetch", "to": "both_strict", "kind": "data", "on": ["Unavailable"]}
        ]
    }))
    .unwrap()
}

#[rstest]
#[case("Rows", S::Success, S::Skipped)]
#[case("Unavailable", S::Skipped, S::Success)]
fn edges_are_taken_on_their_outcomes(
    #[case] outcome: &str,
    #[case] load: TaskInstanceState,
    #[case] report_outage: TaskInstanceState,
) {
    let dag = routed();
    let mut run = RunProgress::new(&dag, BTreeMap::from([(id("fetch"), S::Success)]))
        .with_outcomes(BTreeMap::from([(id("fetch"), outcome.to_owned())]));
    run.settle();
    assert_eq!(run.states()[&id("load")], load);
    assert_eq!(run.states()[&id("report_outage")], report_outage);
    // Two edges, one taken: NoneFailed runs it, AllSucceeded skips it (edges are counted).
    assert_eq!(run.states()[&id("both")], S::Success);
    assert_eq!(run.states()[&id("both_strict")], S::Skipped);
    assert_eq!(run.result(), Some(RunState::Success));
}

#[test]
fn an_outcome_set_after_an_attempt_routes_the_downstream_tasks() {
    let dag = routed();
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    run.settle();
    run.set(&id("fetch"), S::Success);
    run.set_outcome(&id("fetch"), "Unavailable");
    let changed = run.settle();
    assert!(changed.contains(&(id("load"), S::Skipped)), "{changed:?}");
    assert!(
        changed.contains(&(id("report_outage"), S::Success)),
        "{changed:?}"
    );
}

#[test]
fn a_task_without_outcomes_takes_plain_edges_on_success() {
    // No outcome recorded: SUCCESS, which a plain edge (any outcome) takes.
    let dag = dag();
    let mut run = RunProgress::new(&dag, BTreeMap::from([(id("start"), S::Success)]));
    assert_eq!(run.settle(), changes(&[("extract", S::Queued)]));
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

#[test]
fn an_attempt_lost_while_cancelling_is_cancelled_otherwise_it_uses_a_retry() {
    let now = ts("2026-10-21T08:00:00Z");
    assert_eq!(
        after_attempt(AttemptStatus::Lost, None, 0, &settings(), true, now),
        AfterAttempt::Cancelled
    );
    assert!(after_attempt(AttemptStatus::Lost, None, 0, &settings(), false, now).uses_retry());
}

/// `pick` (a `@branch`) selecting among `a`, `b` and `c`; `c` also after `other`.
fn branched() -> Dag {
    let mut pick = task("pick", "python");
    pick["branch"] = json!(true);
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "branched", "location": {"root": "etl", "path": "b.py"},
        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
        "tasks": [pick, task("other", "empty"), task("a", "empty"), task("b", "empty"),
                  task("c", "empty")],
        "edges": [
            {"from": "pick", "to": "a", "kind": "selection"},
            {"from": "pick", "to": "b", "kind": "selection"},
            {"from": "pick", "to": "c", "kind": "selection"},
            {"from": "other", "to": "c", "kind": "order"}
        ]
    }))
    .unwrap()
}

#[rstest]
#[case(&["a", "c"], S::Success, S::Skipped, S::Success)]
// Nothing chosen: every target's selection edge not taken; `c` still runs through `other`.
#[case(&[], S::Skipped, S::Skipped, S::Success)]
#[case(&["b"], S::Skipped, S::Success, S::Success)]
fn a_branch_runs_the_targets_it_chose(
    #[case] chosen: &[&str],
    #[case] a: TaskInstanceState,
    #[case] b: TaskInstanceState,
    #[case] c: TaskInstanceState,
) {
    let dag = branched();
    let mut run = RunProgress::new(&dag, BTreeMap::from([(id("pick"), S::Success)])).with_chosen(
        BTreeMap::from([(id("pick"), chosen.iter().map(|t| id(t)).collect())]),
    );
    run.settle();
    assert_eq!(
        [
            run.states()[&id("a")],
            run.states()[&id("b")],
            run.states()[&id("c")]
        ],
        [a, b, c]
    );
}
