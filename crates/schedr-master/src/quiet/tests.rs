use std::collections::HashSet;

use pretty_assertions::assert_eq;

use super::*;

fn id(name: &str) -> DagId {
    DagId::new(name).unwrap()
}

fn quiet_dags() -> (QuietDags, Wakes, MasterMetrics) {
    let wakes = Wakes::default();
    let metrics = MasterMetrics::default();
    let mut quiet = QuietDags::new(QuietSettings::default(), wakes.clone());
    quiet.set_metrics(metrics.clone());
    (quiet, wakes, metrics)
}

fn some(names: &[&str]) -> Visit {
    Visit::Some(names.iter().map(|n| id(n)).collect())
}

fn seen(names: &[&str]) -> HashSet<DagId> {
    names.iter().map(|n| id(n)).collect()
}

fn wakes_for(metrics: &MasterMetrics, reason: &str) -> u64 {
    metrics
        .dag_wakes
        .get_or_create(&crate::metrics::ReasonLabels {
            reason: reason.to_owned(),
        })
        .get()
}

fn dags_in(metrics: &MasterMetrics, state: &str) -> i64 {
    metrics
        .dags
        .get_or_create(&crate::metrics::StateLabels {
            state: state.to_owned(),
        })
        .get()
}

/// A round over `visit`, each DAG seen and not held.
fn done(quiet: &mut QuietDags, visit: &Visit, names: &[&str]) {
    for name in names {
        quiet.visited(&id(name), Visited { held: None });
    }
    quiet.round_done(visit, &seen(names));
}

#[test]
fn a_terms_first_round_visits_every_dag_then_only_what_is_due_or_marked() {
    // Decision 485: nothing kept in memory decides a visit; after the first round, the DAGs the
    // database found due and the marked ones, each counted by why.
    let (mut quiet, wakes, metrics) = quiet_dags();
    assert_eq!(quiet.round(vec![(id("a"), WakeReason::Timer)]), Visit::All);
    done(&mut quiet, &Visit::All, &["a", "b", "c"]);
    assert_eq!(
        (dags_in(&metrics, "busy"), dags_in(&metrics, "quiet")),
        (3, 0)
    );
    wakes.wake(&id("c"), WakeReason::Command);
    let visit = quiet.round(vec![
        (id("a"), WakeReason::Timer),
        (id("b"), WakeReason::Unknown),
    ]);
    assert_eq!(visit, some(&["a", "b", "c"]));
    done(&mut quiet, &visit, &["a", "b", "c"]);
    assert_eq!(
        (
            wakes_for(&metrics, "timer"),
            wakes_for(&metrics, "unknown"),
            wakes_for(&metrics, "command")
        ),
        (1, 1, 1)
    );
    // Nothing due: nothing visited, every DAG quiet.
    let visit = quiet.round(Vec::new());
    assert_eq!(visit, some(&[]));
    quiet.round_done(&visit, &HashSet::new());
    assert_eq!(
        (dags_in(&metrics, "busy"), dags_in(&metrics, "quiet")),
        (0, 3)
    );
}

#[test]
fn off_visits_every_dag_every_round() {
    let wakes = Wakes::default();
    let mut quiet = QuietDags::new(QuietSettings { enabled: false }, wakes);
    for _ in 0..3 {
        assert_eq!(quiet.round(Vec::new()), Visit::All);
        quiet.round_done(&Visit::All, &seen(&["a"]));
    }
}

#[test]
fn a_failed_round_puts_its_marks_back_and_a_first_one_is_done_again() {
    let (mut quiet, wakes, _) = quiet_dags();
    wakes.wake(&id("a"), WakeReason::RunEnded);
    assert_eq!(quiet.round(Vec::new()), Visit::All);
    quiet.round_failed();
    // Still the term's first round, the mark back.
    assert_eq!(quiet.round(Vec::new()), Visit::All);
    done(&mut quiet, &Visit::All, &["a"]);
    wakes.wake(&id("a"), WakeReason::RunEnded);
    assert_eq!(quiet.round(Vec::new()), some(&["a"]));
    quiet.round_failed();
    assert_eq!(quiet.round(Vec::new()), some(&["a"]));
}

#[test]
fn held_dags_are_counted_while_held_and_forgotten_when_gone() {
    let (mut quiet, _, metrics) = quiet_dags();
    assert_eq!(quiet.round(Vec::new()), Visit::All);
    quiet.visited(&id("a"), Visited { held: Some("a.py") });
    quiet.visited(&id("b"), Visited { held: None });
    quiet.round_done(&Visit::All, &seen(&["a", "b"]));
    assert_eq!(dags_in(&metrics, "held"), 1);
    // `a` can't run any more (removed): forgotten when a round visiting it doesn't see it.
    let visit = quiet.round(vec![(id("a"), WakeReason::Timer)]);
    quiet.round_done(&visit, &HashSet::new());
    assert_eq!(dags_in(&metrics, "held"), 0);
    assert_eq!(dags_in(&metrics, "quiet"), 1, "only `b` known");
}
