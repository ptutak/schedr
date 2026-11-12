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
        "produced_by": "1.0", "id": "sync", "location": {"root": "etl", "path": "a.py"},
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
        "produced_by": "1.0", "id": "routed", "location": {"root": "etl", "path": "routed.py"},
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
    assert_eq!(running, [(id("load"), -1)]);
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
        "produced_by": "1.0", "id": "branched", "location": {"root": "etl", "path": "b.py"},
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

/// A python task for the mapping tests: `expand` over `over` (a task's whole output, or a
/// literal list), with `run_if`.
fn mapped_task(name: &str, over: Option<&Json>, run_if: &str) -> Json {
    let mut task = task(name, "python");
    task["module"] = json!("m");
    task["function"] = json!("f");
    task["arguments"] = json!({});
    task["settings"]["run_if"] = json!(run_if);
    if let Some(over) = over {
        task["expand"] = json!({"product": [{"name": "n", "value": over}]});
    }
    task
}

fn output_of(task: &str) -> Json {
    json!({"ref": {"kind": "output", "task": task}})
}

/// `size` mapped over a literal, `check` over `size` element by element (a lane), `total`
/// gathering `check`; `edges` added to those.
fn mapped_dag(check_run_if: &str, extra_edges: &Json) -> Dag {
    let mut edges = vec![
        json!({"from": "size", "to": "check", "kind": "data"}),
        json!({"from": "check", "to": "total", "kind": "data"}),
    ];
    edges.extend(extra_edges.as_array().unwrap().iter().cloned());
    let mut total = mapped_task("total", None, "none_failed");
    total["arguments"] = json!({"all": output_of("check")});
    serde_json::from_value(json!({
        "produced_by": "1.0", "id": "m", "location": {"root": "etl", "path": "m.py"},
        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
        "tasks": [
            mapped_task("size", Some(&json!({"literal": [1, 2, 3]})), "none_failed"),
            mapped_task("check", Some(&output_of("size")), check_run_if),
            total
        ],
        "edges": edges
    }))
    .unwrap()
}

/// Settles, expanding as asked with `sizes` per task, until nothing changes.
fn settle_expanding(run: &mut RunProgress<'_>, sizes: &BTreeMap<&str, Vec<usize>>) {
    loop {
        run.settle();
        let ready = run.take_expansions();
        if ready.is_empty() {
            return;
        }
        for task in ready {
            let expansion = Expansion::Sizes(sizes[task.as_str()].clone());
            run.expand(&task, expansion);
        }
    }
}

fn instance_states(run: &RunProgress<'_>, task: &str) -> Vec<TaskInstanceState> {
    run.instances(&id(task))
        .map(|i| i.states.clone())
        .unwrap_or_default()
}

#[test]
fn lanes_move_on_their_own_until_the_gather() {
    let dag = mapped_dag("none_failed", &json!([]));
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    let sizes = BTreeMap::from([("size", vec![3]), ("check", vec![3])]);
    settle_expanding(&mut run, &sizes);
    // Both expanded at once: `check`'s count is `size`'s, known when `size` expanded.
    assert_eq!(instance_states(&run, "size"), [S::Queued; 3]);
    assert_eq!(instance_states(&run, "check"), [S::Scheduled; 3]);
    assert_eq!(run.states()[&id("check")], S::Running);
    // `check[1]` starts when `size[1]` is done, whatever the others do.
    run.set_instance(&id("size"), 1, S::Success);
    run.set_instance_outcome(&id("size"), 1, "SUCCESS");
    settle_expanding(&mut run, &sizes);
    assert_eq!(
        instance_states(&run, "check"),
        [S::Scheduled, S::Queued, S::Scheduled]
    );
    // The gather waits for every instance.
    for i in [0, 2] {
        run.set_instance(&id("size"), i, S::Success);
    }
    settle_expanding(&mut run, &sizes);
    for i in 0..3 {
        run.set_instance(&id("check"), i, S::Success);
    }
    assert_eq!(run.states()[&id("total")], S::Scheduled);
    settle_expanding(&mut run, &sizes);
    assert_eq!(run.states()[&id("size")], S::Success);
    assert_eq!(run.states()[&id("check")], S::Success);
    assert_eq!(run.states()[&id("total")], S::Queued);
}

#[test]
fn an_order_edge_beside_the_lane_is_a_barrier() {
    let dag = mapped_dag(
        "none_failed",
        &json!([{"from": "size", "to": "check", "kind": "order"}]),
    );
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    let sizes = BTreeMap::from([("size", vec![2]), ("check", vec![2])]);
    settle_expanding(&mut run, &sizes);
    run.set_instance(&id("size"), 0, S::Success);
    settle_expanding(&mut run, &sizes);
    // `size` as a whole hasn't finished: `check` doesn't even expand.
    assert_eq!(run.states()[&id("check")], S::Scheduled);
    run.set_instance(&id("size"), 1, S::Success);
    settle_expanding(&mut run, &sizes);
    assert_eq!(instance_states(&run, "check"), [S::Queued; 2]);
}

#[test]
fn a_failed_instance_fails_the_step_at_once_and_cancels_the_rest() {
    let dag = mapped_dag("none_failed", &json!([]));
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    let sizes = BTreeMap::from([("size", vec![3]), ("check", vec![3])]);
    settle_expanding(&mut run, &sizes);
    run.set_instance(&id("size"), 0, S::Running);
    run.set_instance(&id("size"), 1, S::Success);
    settle_expanding(&mut run, &sizes);
    run.set_instance(&id("check"), 1, S::Running);
    run.set_instance(&id("size"), 2, S::Failed);
    settle_expanding(&mut run, &sizes);
    // `size` failed: its queued instance is cancelled, its running one stopped.
    assert_eq!(run.states()[&id("size")], S::Failed);
    assert_eq!(
        instance_states(&run, "size"),
        [S::Running, S::Success, S::Failed]
    );
    // `check[2]`'s lane failed, so `check` fails as a step too: `check[0]` (waiting for its
    // lane) is cancelled, the running `check[1]` stopped.
    assert_eq!(run.states()[&id("check")], S::UpstreamFailed);
    assert_eq!(
        instance_states(&run, "check"),
        [S::Cancelled, S::Running, S::UpstreamFailed]
    );
    assert_eq!(run.take_stops(), [(id("size"), 0), (id("check"), 1)]);
    assert_eq!(run.states()[&id("total")], S::UpstreamFailed);
    // The run ends once the stopped instances have.
    assert_eq!(run.result(), None);
    run.set_instance(&id("size"), 0, S::Cancelled);
    run.set_instance(&id("check"), 1, S::Cancelled);
    assert_eq!(run.result(), Some(RunState::Failed));
}

#[rstest]
#[case::none_failed_some_skipped("none_failed", &["Rows", "Missing"], S::Success)]
#[case::none_failed_all_skipped("none_failed", &["Missing", "Missing"], S::Skipped)]
#[case::all_succeeded_one_skipped("all_succeeded", &["Rows", "Missing"], S::Skipped)]
#[case::all_succeeded_none_skipped("all_succeeded", &["Rows", "Rows"], S::Success)]
fn a_mapped_task_applies_its_run_if_to_its_instances(
    #[case] run_if: &str,
    #[case] outcomes: &[&str],
    #[case] expected: TaskInstanceState,
) {
    // `check` maps over `size.on(Rows)`: an instance whose `size` ended `Missing` is skipped.
    let dag = mapped_dag(run_if, &json!([]));
    let mut dag = dag;
    dag.edges[0] = serde_json::from_value(
        json!({"from": "size", "to": "check", "kind": "data", "on": ["Rows"]}),
    )
    .unwrap();
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    let sizes = BTreeMap::from([("size", vec![2]), ("check", vec![2])]);
    settle_expanding(&mut run, &sizes);
    for (i, outcome) in outcomes.iter().enumerate() {
        run.set_instance(&id("size"), i, S::Success);
        run.set_instance_outcome(&id("size"), i, outcome);
    }
    settle_expanding(&mut run, &sizes);
    for (i, outcome) in outcomes.iter().enumerate() {
        if *outcome == "Rows" {
            assert_eq!(instance_states(&run, "check")[i], S::Queued);
            run.set_instance(&id("check"), i, S::Success);
        } else {
            assert_eq!(instance_states(&run, "check")[i], S::Skipped);
        }
    }
    settle_expanding(&mut run, &sizes);
    assert_eq!(run.states()[&id("check")], expected);
}

#[test]
fn an_empty_expansion_succeeds_and_a_failed_one_has_its_reason() {
    let dag = mapped_dag("none_failed", &json!([]));
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    settle_expanding(
        &mut run,
        &BTreeMap::from([("size", vec![0]), ("check", vec![0])]),
    );
    assert_eq!(run.states()[&id("size")], S::Success);
    assert_eq!(run.states()[&id("check")], S::Success);
    assert_eq!(run.states()[&id("total")], S::Queued);

    let mut run = RunProgress::new(&dag, BTreeMap::new());
    run.settle();
    assert_eq!(run.take_expansions(), [id("size")]);
    run.expand(&id("size"), Expansion::Fail("too many".to_owned()));
    run.settle();
    assert_eq!(run.states()[&id("size")], S::Failed);
    assert_eq!(run.reason(&id("size")), Some("too many"));
    assert_eq!(run.states()[&id("check")], S::UpstreamFailed);
}

#[test]
fn an_element_narrowed_from_a_list_also_used_whole_is_skipped_on_another_outcome() {
    // B80 (decision 119): `check.partial(all=sizes).expand(n=sizes.on(Rows))` waits for the
    // whole of `size`, and `.on(Rows)` still goes element by element. The edge's `Rows` was
    // compared with the whole task's own outcome, and every instance was skipped.
    let mut dag = mapped_dag("all_succeeded", &json!([]));
    let narrowed = json!({"ref": {"kind": "output", "task": "size", "on": ["Rows"]}});
    let mut check = mapped_task("check", Some(&narrowed), "all_succeeded");
    check["arguments"] = json!({"all": output_of("size")});
    dag.tasks[1] = serde_json::from_value(check).unwrap();
    dag.edges[0] = serde_json::from_value(
        json!({"from": "size", "to": "check", "kind": "data", "on": ["Rows"]}),
    )
    .unwrap();
    dag.edges.push(
        serde_json::from_value(json!({"from": "size", "to": "check", "kind": "data"})).unwrap(),
    );
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    let sizes = BTreeMap::from([("size", vec![3]), ("check", vec![3])]);
    settle_expanding(&mut run, &sizes);
    run.set_instance(&id("size"), 0, S::Success);
    run.set_instance_outcome(&id("size"), 0, "Rows");
    settle_expanding(&mut run, &sizes);
    // Not a lane: `check` waits for the whole of `size`.
    assert_eq!(run.states()[&id("check")], S::Scheduled);
    for (i, outcome) in [(1, "Missing"), (2, "Rows")] {
        run.set_instance(&id("size"), i, S::Success);
        run.set_instance_outcome(&id("size"), i, outcome);
    }
    settle_expanding(&mut run, &sizes);
    assert_eq!(run.states()[&id("size")], S::Success);
    assert_eq!(
        instance_states(&run, "check"),
        [S::Queued, S::Skipped, S::Queued]
    );
}

#[rstest]
#[case::none_failed_all("none_failed", &["Rows", "Rows", "Rows"], S::Queued)]
#[case::none_failed_some("none_failed", &["Rows", "Missing", "Rows"], S::Queued)]
#[case::none_failed_none("none_failed", &["Missing", "Missing", "Missing"], S::Skipped)]
#[case::all_succeeded_all("all_succeeded", &["Rows", "Rows", "Rows"], S::Queued)]
#[case::all_succeeded_some("all_succeeded", &["Rows", "Missing", "Rows"], S::Skipped)]
#[case::all_succeeded_none("all_succeeded", &["Missing", "Missing", "Missing"], S::Skipped)]
fn an_order_edge_narrowed_from_a_mapped_task_is_decided_per_instance_by_the_rule(
    #[case] run_if: &str,
    #[case] outcomes: &[&str],
    #[case] expected: TaskInstanceState,
) {
    // B132 (decision 120): `sizes.on(Rows) >> report()` compared `Rows` with the whole mapped
    // task's own outcome, and `report` was always skipped. Each instance is an edge now.
    let mut report = mapped_task("report", None, run_if);
    report["arguments"] = json!({});
    let dag: Dag = serde_json::from_value(json!({
        "produced_by": "1.0", "id": "m", "location": {"root": "etl", "path": "m.py"},
        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
        "tasks": [
            mapped_task("size", Some(&json!({"literal": [1, 2, 3]})), "none_failed"),
            report
        ],
        "edges": [{"from": "size", "to": "report", "kind": "order", "on": ["Rows"]}]
    }))
    .unwrap();
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    let sizes = BTreeMap::from([("size", vec![3])]);
    settle_expanding(&mut run, &sizes);
    for (i, outcome) in outcomes.iter().enumerate() {
        run.set_instance(&id("size"), i, S::Success);
        run.set_instance_outcome(&id("size"), i, outcome);
    }
    settle_expanding(&mut run, &sizes);
    assert_eq!(run.states()[&id("size")], S::Success);
    assert_eq!(run.states()[&id("report")], expected);
}

/// `cluster` (a resource's setup) → `load` (a user by its value), `warm` (a user by
/// `uses=`, also after `pick`), `cluster.cleanup` (data edge from the setup); `after` follows
/// the cleanup (a group's wait, decision 266).
fn resourced(cleanup_kind: &str) -> Dag {
    serde_json::from_value(json!({
        "produced_by": "1.0", "id": "res", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "none"}, "initial_since": null,
        "max_active_runs": 1,
        "tasks": [
            task("cluster", "python"), task("pick", "python"), task("load", "python"),
            task("warm", "python"), task("cluster.cleanup", cleanup_kind),
            task("after", "python")
        ],
        "edges": [
            {"from": "cluster", "to": "load", "kind": "data"},
            {"from": "cluster", "to": "warm", "kind": "requirement"},
            {"from": "pick", "to": "warm", "kind": "order"},
            {"from": "cluster", "to": "cluster.cleanup", "kind": "data"},
            {"from": "cluster.cleanup", "to": "after", "kind": "order"}
        ],
        "resources": [{"setup": "cluster", "cleanup": "cluster.cleanup", "users": ["load", "warm"]}]
    }))
    .unwrap()
}

#[test]
fn a_cleanup_waits_for_every_user_then_what_follows_it_waits_for_the_cleanup() {
    let dag = resourced("python");
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    assert_eq!(
        run.settle(),
        changes(&[("cluster", S::Queued), ("pick", S::Queued)])
    );
    run.set(&id("cluster"), S::Success);
    run.set(&id("pick"), S::Success);
    assert_eq!(
        run.settle(),
        changes(&[("load", S::Queued), ("warm", S::Queued)])
    );
    run.set(&id("load"), S::Failed);
    assert_eq!(run.settle(), []);
    run.set(&id("warm"), S::Unschedulable);
    assert_eq!(run.settle(), [], "an unschedulable user isn't done");
    run.set(&id("warm"), S::Success);
    assert_eq!(run.settle(), changes(&[("cluster.cleanup", S::Queued)]));
    run.set(&id("cluster.cleanup"), S::Running);
    assert_eq!(run.settle(), []);
    run.set(&id("cluster.cleanup"), S::Success);
    assert_eq!(run.settle(), changes(&[("after", S::Queued)]));
}

#[test]
fn a_requirement_only_stops_its_user() {
    let dag = resourced("python");
    // Under `NoneFailed` the setup's success doesn't make `warm` run: `pick` skipped decides.
    let states = BTreeMap::from([(id("cluster"), S::Success), (id("pick"), S::Skipped)]);
    let mut run = RunProgress::new(&dag, states);
    assert!(run.settle().contains(&(id("warm"), S::Skipped)));
    // The setup skipped skips the user whatever its other edges say.
    let states = BTreeMap::from([(id("cluster"), S::Skipped), (id("pick"), S::Success)]);
    let mut run = RunProgress::new(&dag, states);
    let moved = run.settle();
    assert!(moved.contains(&(id("warm"), S::Skipped)), "{moved:?}");
    assert!(
        moved.contains(&(id("cluster.cleanup"), S::Skipped)),
        "{moved:?}"
    );
    // The setup failed fails it.
    let states = BTreeMap::from([(id("cluster"), S::Failed), (id("pick"), S::Success)]);
    let mut run = RunProgress::new(&dag, states);
    let moved = run.settle();
    assert!(
        moved.contains(&(id("warm"), S::UpstreamFailed)),
        "{moved:?}"
    );
    assert!(
        moved.contains(&(id("cluster.cleanup"), S::UpstreamFailed)),
        "{moved:?}"
    );
}

#[test]
fn an_empty_cleanup_is_finished_by_the_master_with_a_reason() {
    let dag = resourced("empty");
    let states = BTreeMap::from([
        (id("cluster"), S::Success),
        (id("pick"), S::Success),
        (id("load"), S::Success),
        (id("warm"), S::Success),
    ]);
    let mut run = RunProgress::new(&dag, states);
    assert_eq!(
        run.settle(),
        changes(&[("cluster.cleanup", S::Success), ("after", S::Queued)])
    );
    assert_eq!(run.reason(&id("cluster.cleanup")), Some(EMPTY_CLEANUP));
}

#[test]
fn cancelling_a_run_keeps_the_cleanup_of_a_resource_set_up() {
    let dag = resourced("python");
    let states = BTreeMap::from([
        (id("cluster"), S::Success),
        (id("pick"), S::Success),
        (id("load"), S::Running),
        (id("warm"), S::Queued),
    ]);
    let mut run = RunProgress::new(&dag, states);
    let (cancelled, running) = run.cancel();
    assert_eq!(
        cancelled,
        changes(&[("after", S::Cancelled), ("warm", S::Cancelled)])
    );
    assert_eq!(running, [(id("load"), -1)]);
    assert_eq!(run.settle(), []);
    run.set(&id("load"), S::Cancelled);
    assert_eq!(run.settle(), changes(&[("cluster.cleanup", S::Queued)]));
    // A running cleanup isn't stopped either.
    run.set(&id("cluster.cleanup"), S::Running);
    assert_eq!(run.cancel().1, []);
    run.set(&id("cluster.cleanup"), S::Success);
    assert_eq!(run.result(), Some(RunState::Cancelled));
}

#[test]
fn cancelling_before_the_setup_ran_leaves_nothing_to_clean() {
    let dag = resourced("python");
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    run.settle();
    run.cancel();
    let moved = run.settle();
    assert!(
        moved.contains(&(id("cluster.cleanup"), S::Cancelled)),
        "{moved:?}"
    );
    assert_eq!(run.result(), Some(RunState::Cancelled));
}

#[test]
fn a_failed_cleanup_fails_the_run() {
    let dag = resourced("python");
    let states = BTreeMap::from([
        (id("cluster"), S::Success),
        (id("pick"), S::Success),
        (id("load"), S::Success),
        (id("warm"), S::Success),
        (id("cluster.cleanup"), S::Failed),
    ]);
    let mut run = RunProgress::new(&dag, states);
    assert_eq!(run.settle(), changes(&[("after", S::UpstreamFailed)]));
    assert_eq!(run.result(), Some(RunState::Failed));
}

#[test]
fn a_cleanup_waits_for_a_failed_mapped_users_instances_still_running() {
    // Codex's review of M4.8c2: the user failed as a whole while an instance still ran.
    let dag = resourced("python");
    let states = BTreeMap::from([
        (id("cluster"), S::Success),
        (id("pick"), S::Success),
        (id("load"), S::Failed),
        (id("warm"), S::Success),
    ]);
    let instances = BTreeMap::from([(
        id("load"),
        Instances {
            sizes: vec![2],
            states: vec![S::Failed, S::Running],
            outcomes: vec![None, None],
        },
    )]);
    let mut run = RunProgress::new(&dag, states).with_instances(instances);
    assert_eq!(run.settle(), []);
    run.set_instance(&id("load"), 1, S::Cancelled);
    assert_eq!(run.settle(), changes(&[("cluster.cleanup", S::Queued)]));
}

#[test]
fn a_run_cancelled_with_only_a_cleanup_left_ends_cancelled() {
    // Codex's review of M4.8c2: nothing left to cancel, it ended `success`.
    let dag = resourced("python");
    let states = BTreeMap::from([
        (id("cluster"), S::Success),
        (id("pick"), S::Success),
        (id("load"), S::Success),
        (id("warm"), S::Success),
        (id("cluster.cleanup"), S::Running),
        (id("after"), S::Success),
    ]);
    let mut run = RunProgress::new(&dag, states);
    assert_eq!(run.cancel(), (Vec::new(), Vec::new()));
    run.set(&id("cluster.cleanup"), S::Success);
    assert_eq!(run.result(), Some(RunState::Cancelled));
}

#[rstest]
#[case::one_is_enough(&[S::Success, S::Running], 1, Readiness::Ready)]
#[case::the_other_is_still_running(&[S::Skipped, S::Running], 1, Readiness::Wait)]
#[case::a_failure_with_one_left(&[S::Failed, S::Running], 1, Readiness::Wait)]
#[case::a_success_beats_a_failure(&[S::Failed, S::Success], 1, Readiness::Ready)]
#[case::none_left_one_failed(&[S::Failed, S::Skipped], 1, Readiness::UpstreamFailed)]
#[case::none_left_upstream_failed(&[S::UpstreamFailed, S::Skipped], 1, Readiness::UpstreamFailed)]
#[case::none_left_one_cancelled(&[S::Cancelled, S::Skipped], 1, Readiness::Cancel)]
#[case::a_failure_outweighs_a_cancel(&[S::Cancelled, S::Failed], 1, Readiness::UpstreamFailed)]
#[case::all_skipped(&[S::Skipped, S::Skipped], 1, Readiness::Skip)]
#[case::two_needed_one_there(&[S::Success, S::Running, S::Running], 2, Readiness::Wait)]
#[case::two_needed_two_there(&[S::Success, S::Success, S::Running], 2, Readiness::Ready)]
#[case::two_needed_one_can_only(&[S::Success, S::Failed, S::Skipped], 2, Readiness::UpstreamFailed)]
#[case::decided_before_the_last_ends(&[S::Failed, S::Failed, S::Running], 2, Readiness::UpstreamFailed)]
fn what_a_first_of_waits_for(
    #[case] edges: &[TaskInstanceState],
    #[case] need: usize,
    #[case] decided: Readiness,
) {
    assert_eq!(first_of_readiness(edges, need), decided);
}

/// A chain of `n` empty tasks `t0 → t1 → …`, listed last first.
fn reversed_chain(n: usize) -> Dag {
    let tasks: Vec<Json> = (0..n)
        .rev()
        .map(|i| task(&format!("t{i}"), "empty"))
        .collect();
    let edges: Vec<Json> = (1..n)
        .map(|i| json!({"from": format!("t{}", i - 1), "to": format!("t{i}"), "kind": "order"}))
        .collect();
    serde_json::from_value(json!({
        "produced_by": "1.0", "id": "chain", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
        "tasks": tasks, "edges": edges
    }))
    .unwrap()
}

#[test]
fn a_chain_listed_last_first_settles_in_one_pass() {
    // B82: one task a pass, each pass every task's edges searched: cubic on a chain.
    let dag = reversed_chain(300);
    let mut run = RunProgress::new(&dag, BTreeMap::new());
    let changed = run.settle();
    assert_eq!(changed.len(), 300, "every empty task completed");
    assert!(changed.iter().all(|(_, state)| *state == S::Success));
    assert!(run.passes <= 2, "{} passes over the tasks", run.passes);
}
