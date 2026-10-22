use pretty_assertions::assert_eq;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn queues(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| (*n).to_owned()).collect()
}

fn none() -> BTreeSet<String> {
    BTreeSet::new()
}

#[test]
fn a_worker_without_queues_serves_the_default_queue_only() {
    let mut workers = Workers::new();
    assert!(workers.register("w1", "i1", &[], 2));
    let w1 = workers.get("w1").unwrap();
    assert!(w1.serves(DEFAULT_QUEUE));
    assert!(!w1.serves("large"));
    workers.register("w2", "i1", &queues(&["large"]), 2);
    assert!(!workers.get("w2").unwrap().serves(DEFAULT_QUEUE));
    assert_eq!(workers.pick(Needs::queue("large"), &none()), Some("w2"));
    assert_eq!(workers.pick(Needs::queue("gpu"), &none()), None);
}

#[test]
fn the_worker_with_the_most_free_slots_gets_the_task() {
    let mut workers = Workers::new();
    workers.register("w1", "i1", &[], 4);
    workers.register("w2", "i1", &[], 2);
    assert_eq!(
        workers.pick(Needs::queue(DEFAULT_QUEUE), &none()),
        Some("w1")
    );
    workers.reserve("w1", "a1", ts("2026-10-21T08:00:00Z"));
    workers.reserve("w1", "a2", ts("2026-10-21T08:00:01Z"));
    // Both have 2 free slots: w2 was never given a task.
    assert_eq!(
        workers.pick(Needs::queue(DEFAULT_QUEUE), &none()),
        Some("w2")
    );
    workers.reserve("w2", "a3", ts("2026-10-21T08:00:02Z"));
    workers.reserve("w2", "a4", ts("2026-10-21T08:00:03Z"));
    workers.reserve("w1", "a5", ts("2026-10-21T08:00:04Z"));
    workers.reserve("w1", "a6", ts("2026-10-21T08:00:05Z"));
    assert_eq!(workers.get("w1").unwrap().free_slots(), 0);
    assert_eq!(workers.pick(Needs::queue(DEFAULT_QUEUE), &none()), None);
    workers.release("a3");
    assert_eq!(
        workers.pick(Needs::queue(DEFAULT_QUEUE), &none()),
        Some("w2")
    );
}

#[test]
fn ties_go_to_the_least_recently_assigned_then_the_id() {
    let mut workers = Workers::new();
    workers.register("w2", "i1", &[], 1);
    workers.register("w1", "i1", &[], 1);
    assert_eq!(
        workers.pick(Needs::queue(DEFAULT_QUEUE), &none()),
        Some("w1")
    );
    workers.reserve("w1", "a1", ts("2026-10-21T08:00:00Z"));
    workers.release("a1");
    assert_eq!(
        workers.pick(Needs::queue(DEFAULT_QUEUE), &none()),
        Some("w2")
    );
}

#[test]
fn excluded_and_draining_workers_get_nothing() {
    let mut workers = Workers::new();
    workers.register("w1", "i1", &[], 2);
    workers.register("w2", "i1", &[], 2);
    let excluded = BTreeSet::from(["w1".to_owned()]);
    assert_eq!(
        workers.pick(Needs::queue(DEFAULT_QUEUE), &excluded),
        Some("w2")
    );
    workers.draining("w2");
    assert_eq!(workers.pick(Needs::queue(DEFAULT_QUEUE), &excluded), None);
    assert_eq!(workers.serving(DEFAULT_QUEUE).count(), 1);
    workers.draining("ghost");
}

#[test]
fn registering_again_keeps_attempts_but_a_new_incarnation_starts_afresh() {
    let mut workers = Workers::new();
    workers.register("w1", "i1", &[], 2);
    workers.reserve("w1", "a1", ts("2026-10-21T08:00:00Z"));
    workers.draining("w1");
    assert!(!workers.register("w1", "i1", &queues(&["large"]), 3));
    let w1 = workers.get("w1").unwrap();
    assert_eq!(
        (w1.state, w1.slots, w1.attempts.len()),
        (WorkerState::Active, 3, 1)
    );
    assert!(w1.serves("large"));
    assert!(workers.register("w1", "i2", &[], 2));
    assert_eq!(workers.get("w1").unwrap().attempts.len(), 0);
}

#[test]
fn removed_workers_are_forgotten() {
    let mut workers = Workers::new();
    workers.register("w1", "i1", &[], 2);
    assert_eq!(workers.remove("w1").unwrap().id, "w1");
    assert_eq!(workers.get("w1"), None);
    assert_eq!(workers.remove("w1"), None);
    workers.reserve("w1", "a1", ts("2026-10-21T08:00:00Z"));
}

#[test]
fn only_workers_with_the_current_code_and_a_recent_enough_version_are_eligible() {
    let mut workers = Workers::new();
    for id in ["w1", "w2", "w3"] {
        workers.register(id, "i1", &[], 2);
    }
    workers.set_version("w1", Some(SchedrVersion::new(0, 1)));
    workers.set_version("w2", Some(SchedrVersion::new(0, 2)));
    workers.set_version("ghost", Some(SchedrVersion::new(0, 2)));
    workers.report_root("w1", "etl", "h2");
    workers.report_root("w2", "etl", "h2");
    workers.report_root("w3", "etl", "h1");
    workers.report_root("ghost", "etl", "h2");
    let ids = |needs: Needs<'_>| -> Vec<String> {
        workers.eligible(needs).map(|w| w.id.clone()).collect()
    };
    let h2 = BTreeSet::from(["h2".to_owned()]);
    let newest = Needs {
        code: Some(("etl", &h2)),
        ..Needs::queue(DEFAULT_QUEUE)
    };
    assert_eq!(ids(newest), ["w1", "w2"]);
    let upgraded = Needs {
        version: Some(SchedrVersion::new(0, 2)),
        ..newest
    };
    assert_eq!(
        ids(upgraded),
        ["w2"],
        "w3's version is unknown, but its code is old"
    );
    assert_eq!(workers.pick(upgraded, &none()), Some("w2"));
    let h9 = BTreeSet::from(["h9".to_owned()]);
    let other_root = Needs {
        code: Some(("ml", &h9)),
        ..Needs::queue(DEFAULT_QUEUE)
    };
    assert_eq!(ids(other_root), Vec::<String>::new());
    assert_eq!(ids(Needs::queue(DEFAULT_QUEUE)), ["w1", "w2", "w3"]);
}
