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

#[test]
fn a_drained_worker_gets_nothing_until_resumed_and_a_new_incarnation_starts_undrained() {
    let mut workers = Workers::new();
    workers.register("w1", "i1", &[], 2);
    workers.set_drained("w1", true);
    workers.set_drained("ghost", true);
    assert_eq!(workers.pick(Needs::queue(DEFAULT_QUEUE), &none()), None);
    workers.register("w1", "i1", &[], 2);
    assert!(workers.get("w1").unwrap().drained, "a reconnect keeps it");
    workers.set_drained("w1", false);
    assert_eq!(
        workers.pick(Needs::queue(DEFAULT_QUEUE), &none()),
        Some("w1")
    );
    workers.set_drained("w1", true);
    workers.register("w1", "i2", &[], 2);
    assert!(
        !workers.get("w1").unwrap().drained,
        "the master applies it again"
    );
}

fn queueing() -> Needs<'static> {
    Needs {
        may_queue: true,
        ..Needs::queue(DEFAULT_QUEUE)
    }
}

#[test]
fn free_slots_are_filled_before_any_queue() {
    // Decision 509: a worker with a queue takes queued work only when no worker has a slot.
    let mut workers = Workers::new();
    workers.register("w1", "i1", &[], 1);
    workers.set_queue_depth("w1", 2);
    workers.register("w2", "i1", &[], 1);
    workers.reserve("w1", "a1", ts("2026-10-21T08:00:00Z"));
    // w1 has queue room (2) and no slot; w2 a slot: w2.
    assert_eq!(workers.pick(queueing(), &none()), Some("w2"));
    workers.reserve("w2", "a2", ts("2026-10-21T08:00:01Z"));
    // No slot anywhere: w1's queue; w2 has none.
    assert_eq!(workers.pick(queueing(), &none()), Some("w1"));
    workers.reserve("w1", "a3", ts("2026-10-21T08:00:02Z"));
    workers.reserve("w1", "a4", ts("2026-10-21T08:00:03Z"));
    // Its queue full: nowhere.
    assert_eq!(workers.pick(queueing(), &none()), None);
}

#[test]
fn a_task_that_may_not_queue_goes_to_a_free_slot_only() {
    // Decision 509: one given back, a wait step, a cleanup moved: never into a queue.
    let mut workers = Workers::new();
    workers.register("w1", "i1", &[], 1);
    workers.set_queue_depth("w1", 4);
    workers.reserve("w1", "a1", ts("2026-10-21T08:00:00Z"));
    assert_eq!(workers.pick(Needs::queue(DEFAULT_QUEUE), &none()), None);
    assert_eq!(workers.pick(queueing(), &none()), Some("w1"));
}

#[test]
fn among_queues_the_one_with_the_most_room_gets_the_task() {
    let mut workers = Workers::new();
    for id in ["w1", "w2"] {
        workers.register(id, "i1", &[], 1);
        workers.set_queue_depth(id, 2);
    }
    workers.reserve("w1", "a1", ts("2026-10-21T08:00:00Z"));
    workers.reserve("w1", "a2", ts("2026-10-21T08:00:01Z"));
    workers.reserve("w2", "a3", ts("2026-10-21T08:00:02Z"));
    // w1's queue holds one, w2's none.
    assert_eq!(workers.pick(queueing(), &none()), Some("w2"));
}

#[test]
fn a_worker_without_a_queue_takes_nothing_past_its_slots() {
    let mut workers = Workers::new();
    workers.register("w1", "i1", &[], 1);
    workers.reserve("w1", "a1", ts("2026-10-21T08:00:00Z"));
    assert_eq!(workers.pick(queueing(), &none()), None);
}

/// B285: a root's code is new for a worker only when its hash differs from the one it last
/// reported, a reconnect (the same incarnation, its roots cleared) reporting the same hash
/// included; a withdrawn root's next report is new.
#[test]
fn new_code_is_a_hash_other_than_the_last_reported() {
    let mut workers = Workers::new();
    assert!(!workers.known_as("w1", "i1"));
    workers.register("w1", "i1", &queues(&[]), 2);
    assert!(workers.known_as("w1", "i1"));
    assert!(!workers.known_as("w1", "i2"));
    assert!(workers.report_root("w1", "etl", "h1"), "first report");
    assert!(!workers.report_root("w1", "etl", "h1"), "the same");
    assert!(!workers.register("w1", "i1", &queues(&[]), 2));
    assert!(
        !workers.report_root("w1", "etl", "h1"),
        "reconnected, the same"
    );
    assert!(workers.report_root("w1", "etl", "h2"), "new");
    workers.withdraw_root("w1", "etl");
    assert!(
        workers.report_root("w1", "etl", "h2"),
        "withdrawn, then back"
    );
    assert!(!workers.report_root("w9", "etl", "h1"), "unknown worker");
}

/// B284 (DN37): a service refusal is answered by a count of readies past the one it saw; a
/// lower count, handled after a higher one, takes nothing back; an unknown extension or worker
/// answers nothing.
#[test]
fn a_service_refusal_is_answered_by_a_higher_count_of_readies() {
    let mut workers = Workers::new();
    workers.register("w1", "i1", &queues(&[]), 2);
    assert!(
        !workers.service_ready_since("w1", "spark", 0),
        "nothing heard"
    );
    workers.report_services("w1", BTreeMap::from([("spark".to_owned(), 1)]));
    assert!(
        !workers.service_ready_since("w1", "spark", 1),
        "the count it saw"
    );
    assert!(workers.service_ready_since("w1", "spark", 0));
    workers.report_services("w1", BTreeMap::from([("spark".to_owned(), 0)]));
    assert!(
        workers.service_ready_since("w1", "spark", 0),
        "an older heartbeat"
    );
    assert!(!workers.service_ready_since("w1", "other", 0));
    assert!(!workers.service_ready_since("w9", "spark", 0));
}

/// DN37: a worker's counts are those its last heartbeat named (agy's review: merged, a worker
/// naming new extensions each time grew the map for good); whether one rose.
#[test]
fn a_workers_counts_are_its_last_heartbeats_and_say_whether_one_rose() {
    let mut workers = Workers::new();
    workers.register("w1", "i1", &queues(&[]), 2);
    let named = |pairs: &[(&str, u64)]| -> BTreeMap<String, u64> {
        pairs.iter().map(|(n, c)| ((*n).to_owned(), *c)).collect()
    };
    assert!(workers.report_services("w1", named(&[("a", 1), ("b", 0)])));
    assert!(
        !workers.report_services("w1", named(&[("a", 1), ("b", 0)])),
        "the same"
    );
    assert!(
        !workers.report_services("w1", named(&[("a", 0)])),
        "lower: kept higher"
    );
    assert_eq!(
        workers.get("w1").unwrap().service_readies,
        named(&[("a", 1)]),
        "only the last heartbeat's"
    );
    assert!(workers.report_services("w1", named(&[("a", 2)])));
}

#[test]
fn a_worker_offers_exactly_the_versions_it_registered() {
    // Decision 527: matched exactly; a registration's offers replace the last ones.
    let mut workers = Workers::new();
    workers.register("w1", "i1", &[], 2);
    workers.set_extensions(
        "w1",
        BTreeSet::from([
            ("spark".to_owned(), "1.0".to_owned()),
            ("spark".to_owned(), "1.1".to_owned()),
        ]),
    );
    let worker = workers.get("w1").unwrap();
    assert!(worker.offers(None), "nothing asked: any worker");
    assert!(worker.offers(Some(("spark", "1.1"))));
    assert!(!worker.offers(Some(("spark", "2.0"))));
    assert!(!worker.offers(Some(("flink", "1.0"))));
    workers.set_extensions("w1", BTreeSet::new());
    assert!(!workers.get("w1").unwrap().offers(Some(("spark", "1.0"))));
}
