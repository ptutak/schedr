use pretty_assertions::assert_eq;

use super::*;
use crate::metrics::{ReasonLabels, StateLabels};

const T0: &str = "2026-10-27T12:00:00Z";

fn at(seconds: i64) -> Timestamp {
    T0.parse::<Timestamp>().unwrap() + SignedDuration::from_secs(seconds)
}

fn at_ms(millis: i64) -> Timestamp {
    T0.parse::<Timestamp>().unwrap() + SignedDuration::from_millis(millis)
}

fn id(name: &str) -> DagId {
    DagId::new(name).unwrap()
}

/// What a visit finds of a DAG (its version comes from the round's list).
#[derive(Clone, Copy)]
struct Found {
    active: bool,
    paused: bool,
    next_fire: Option<Timestamp>,
}

const IDLE: Found = Found {
    active: false,
    paused: false,
    next_fire: None,
};

const RUNNING: Found = Found {
    active: true,
    ..IDLE
};

fn quiet_dags() -> (QuietDags, Wakes, MasterMetrics) {
    let wakes = Wakes::default();
    let metrics = MasterMetrics::default();
    let mut quiet = QuietDags::new(QuietSettings::default(), wakes.clone());
    quiet.set_metrics(metrics.clone());
    (quiet, wakes, metrics)
}

/// One round at `now` as the scheduler does it, `active` being the DAGs that can run (name,
/// version): the ID check when due, each visited DAG found as `found` says. The DAGs visited.
fn round(
    quiet: &mut QuietDags,
    now: Timestamp,
    active: &[(&str, &str)],
    found: impl Fn(&str) -> Found,
) -> Visit {
    let round = quiet.round(now);
    let visit = match round.visit {
        Visit::Some(mut dags) if round.check => {
            let versions: Vec<(DagId, String)> = active
                .iter()
                .map(|(n, v)| (id(n), (*v).to_owned()))
                .collect();
            dags.extend(quiet.checked(&versions));
            dags.sort();
            dags.dedup();
            Visit::Some(dags)
        }
        visit => visit,
    };
    let names: Vec<String> = match &visit {
        Visit::All => active.iter().map(|(n, _)| (*n).to_owned()).collect(),
        Visit::Some(dags) => dags.iter().map(ToString::to_string).collect(),
    };
    let mut seen = HashSet::new();
    for name in names {
        if let Some((_, version)) = active.iter().find(|(n, _)| *n == name) {
            let f = found(&name);
            let visited = Visited {
                version,
                active: f.active,
                paused: f.paused,
                next_fire: f.next_fire,
            };
            quiet.visited(&id(&name), visited, now);
            seen.insert(id(&name));
        }
    }
    quiet.round_done(&visit, &seen);
    visit
}

fn some(names: &[&str]) -> Visit {
    Visit::Some(names.iter().map(|n| id(n)).collect())
}

fn v1(names: &[&'static str]) -> Vec<(&'static str, &'static str)> {
    names.iter().map(|n| (*n, "v1")).collect()
}

fn wakes_for(metrics: &MasterMetrics, reason: &str) -> u64 {
    metrics
        .dag_wakes
        .get_or_create(&ReasonLabels {
            reason: reason.to_owned(),
        })
        .get()
}

fn dags_in(metrics: &MasterMetrics, state: &str) -> (i64, i64) {
    let get = |state: &str| {
        metrics
            .dags
            .get_or_create(&StateLabels {
                state: state.to_owned(),
            })
            .get()
    };
    let _ = state;
    (get("busy"), get("quiet"))
}

fn counts(metrics: &MasterMetrics) -> (i64, i64) {
    dags_in(metrics, "")
}

#[test]
fn a_dag_found_idle_is_quiet_at_once_then_visited_by_its_timer_and_the_sweep() {
    let (mut quiet, _, metrics) = quiet_dags();
    let dags = v1(&["daily"]);
    // Nothing in progress; it fires next at 3660 s.
    let daily = |_: &str| Found {
        next_fire: Some(at(3660)),
        ..IDLE
    };
    // The term's first round visits every DAG; idle, with no fire near, it is quiet at once.
    assert_eq!(round(&mut quiet, at(0), &dags, daily), Visit::All);
    assert_eq!(counts(&metrics), (0, 1));
    assert_eq!(round(&mut quiet, at(1), &dags, daily), some(&[]));
    assert_eq!(round(&mut quiet, at(59), &dags, daily), some(&[]));
    // The sweep visits it once per interval; finding nothing to do, it stays quiet and
    // isn't counted as a wake.
    assert_eq!(round(&mut quiet, at(60), &dags, daily), some(&["daily"]));
    assert_eq!(round(&mut quiet, at(61), &dags, daily), some(&[]));
    assert_eq!(round(&mut quiet, at(3599), &dags, daily), some(&["daily"]));
    assert_eq!(counts(&metrics), (0, 1));
    assert_eq!(wakes_for(&metrics, "sweep"), 0);
    // Its timer: 60 s before the fire time; the fire near, it is busy.
    assert_eq!(round(&mut quiet, at(3600), &dags, daily), some(&["daily"]));
    assert_eq!(wakes_for(&metrics, "timer"), 1);
    assert_eq!(counts(&metrics), (1, 0));
}

#[test]
fn a_fire_time_coming_up_or_a_run_in_progress_keeps_a_dag_busy() {
    // Every 30 s: the next fire is always within `quiet_dag_wake_ahead`.
    let (mut quiet, _, metrics) = quiet_dags();
    let dags = v1(&["often"]);
    for second in (0..=300).step_by(10) {
        let next = at((second / 30 + 1) * 30);
        let often = |_: &str| Found {
            next_fire: Some(next),
            ..IDLE
        };
        round(&mut quiet, at(second), &dags, often);
    }
    assert_eq!(counts(&metrics), (1, 0));
    // A run in progress for minutes, then nothing: quiet only 60 s after it was last seen.
    let (mut quiet, _, metrics) = quiet_dags();
    let dags = v1(&["long"]);
    for second in [0, 100, 200] {
        round(&mut quiet, at(second), &dags, |_| RUNNING);
    }
    round(&mut quiet, at(259), &dags, |_| IDLE);
    assert_eq!(counts(&metrics), (1, 0));
    round(&mut quiet, at(260), &dags, |_| IDLE);
    assert_eq!(counts(&metrics), (0, 1));
}

#[test]
fn a_mark_wakes_a_quiet_dag_and_counts_its_reason() {
    let (mut quiet, wakes, metrics) = quiet_dags();
    let dags = v1(&["a", "b"]);
    round(&mut quiet, at(0), &dags, |_| IDLE);
    round(&mut quiet, at(60), &dags, |_| IDLE);
    assert_eq!(counts(&metrics), (0, 2));
    // A mark on a quiet DAG: visited, busy again; the first reason in a round counts.
    wakes.wake(&id("a"), WakeReason::RunQueued);
    wakes.wake(&id("a"), WakeReason::Command);
    assert_eq!(round(&mut quiet, at(61), &dags, |_| RUNNING), some(&["a"]));
    assert_eq!(
        (
            wakes_for(&metrics, "run_queued"),
            wakes_for(&metrics, "command")
        ),
        (1, 0)
    );
    assert_eq!(counts(&metrics), (1, 1));
    // A mark on a busy DAG: visited anyway, nothing counted.
    wakes.wake(&id("a"), WakeReason::RunEnded);
    assert_eq!(round(&mut quiet, at(62), &dags, |_| IDLE), some(&["a"]));
    assert_eq!(wakes_for(&metrics, "run_ended"), 0);
    // A mark on a DAG not known yet (a new one): visited; idle, it is quiet at once.
    wakes.wake(&id("new"), WakeReason::NewVersion);
    let with_new = v1(&["a", "b", "new"]);
    assert_eq!(
        round(&mut quiet, at(63), &with_new, |_| IDLE),
        some(&["a", "new"])
    );
    assert_eq!(counts(&metrics), (1, 2));
    // A command on a quiet DAG with nothing to do (a pause): counted, still quiet.
    wakes.wake(&id("b"), WakeReason::Command);
    assert_eq!(
        round(&mut quiet, at(64), &with_new, |_| IDLE),
        some(&["a", "b"])
    );
    assert_eq!(wakes_for(&metrics, "command"), 1);
    assert_eq!(counts(&metrics), (1, 2));
}

/// 120 quiet DAGs, `d000` to `d119`, all last visited at 60 s.
fn many_quiet() -> (QuietDags, MasterMetrics, Vec<(&'static str, &'static str)>) {
    let (mut quiet, _, metrics) = quiet_dags();
    let names: Vec<&'static str> = (0..120)
        .map(|n| &*Box::leak(format!("d{n:03}").into_boxed_str()))
        .collect();
    let dags = v1(&names);
    round(&mut quiet, at(0), &dags, |_| IDLE);
    round(&mut quiet, at(60), &dags, |_| IDLE);
    assert_eq!(counts(&metrics), (0, 120));
    (quiet, metrics, dags)
}

fn names(visit: &Visit) -> Vec<String> {
    match visit {
        Visit::All => panic!("a round visiting every DAG"),
        Visit::Some(dags) => dags.iter().map(ToString::to_string).collect(),
    }
}

#[test]
fn the_sweep_visits_every_quiet_dag_once_per_interval_oldest_first() {
    let (mut quiet, metrics, dags) = many_quiet();
    // Rounds every second: 120 DAGs over 60 s is 2 a round, each DAG once.
    let mut swept = Vec::new();
    for second in 61..=120 {
        let visited = names(&round(&mut quiet, at(second), &dags, |_| IDLE));
        assert_eq!(visited.len(), 2, "at {second} s: {visited:?}");
        swept.extend(visited);
    }
    let mut unique = swept.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 120);
    // Oldest first (all visited at 60 s: by ID), then the first swept are the oldest again.
    assert_eq!(swept[..4], ["d000", "d001", "d002", "d003"]);
    assert_eq!(
        names(&round(&mut quiet, at(121), &dags, |_| IDLE)),
        ["d000", "d001"]
    );
    assert_eq!(counts(&metrics), (0, 120));
    // A swept DAG with something to do (a missed mark) is counted, and busy.
    let found = |name: &str| if name == "d002" { RUNNING } else { IDLE };
    round(&mut quiet, at(122), &dags, found);
    assert_eq!(wakes_for(&metrics, "sweep"), 1);
    assert_eq!(counts(&metrics), (1, 119));
}

#[test]
fn the_sweep_does_the_same_work_however_many_rounds_there_are() {
    let (mut quiet, _, dags) = many_quiet();
    // Ten rounds a second (commands waking them): still 120 DAGs swept in 60 s.
    let mut swept = 0;
    for tenth in 601..=1200 {
        swept += names(&round(&mut quiet, at_ms(tenth * 100), &dags, |_| IDLE)).len();
    }
    assert_eq!(swept, 120);
    // A round long after the last (a stall): every quiet DAG, once.
    let visited = names(&round(&mut quiet, at(1000), &dags, |_| IDLE));
    assert_eq!(visited.len(), 120);
    // The clock stepped back: nothing swept.
    assert_eq!(
        names(&round(&mut quiet, at(900), &dags, |_| IDLE)),
        Vec::<String>::new()
    );
}

#[test]
fn the_id_check_visits_dags_memory_doesnt_know_and_forgets_gone_ones() {
    let (mut quiet, _, metrics) = quiet_dags();
    let dags = v1(&["a", "b"]);
    round(&mut quiet, at(0), &dags, |_| IDLE);
    round(&mut quiet, at(60), &dags, |_| IDLE);
    assert_eq!(counts(&metrics), (0, 2));
    // By the next check (at 120 s): `new` appeared and `b` changed with their marks missed,
    // `a` is gone.
    let now = [("b", "v2"), ("new", "v1")];
    for second in 61..120 {
        round(&mut quiet, at(second), &now, |_| IDLE);
    }
    let visited = names(&round(&mut quiet, at(120), &now, |_| IDLE));
    assert!(visited.contains(&"new".to_owned()), "{visited:?}");
    assert!(visited.contains(&"b".to_owned()), "{visited:?}");
    // `new` and `b` visited, nothing to do: quiet; `a` forgotten.
    assert_eq!(counts(&metrics), (0, 2));
}

#[test]
fn a_paused_dag_turns_quiet_without_a_timer() {
    let (mut quiet, wakes, metrics) = quiet_dags();
    let dags = v1(&["p"]);
    let paused = |_: &str| Found {
        paused: true,
        next_fire: Some(at(90)),
        ..IDLE
    };
    round(&mut quiet, at(0), &dags, paused);
    round(&mut quiet, at(60), &dags, paused);
    assert_eq!(counts(&metrics), (0, 1));
    // Its fire time passes unvisited; a mark (an unpause) visits it.
    assert_eq!(round(&mut quiet, at(90), &dags, paused), some(&[]));
    wakes.wake(&id("p"), WakeReason::Command);
    assert_eq!(round(&mut quiet, at(91), &dags, paused), some(&["p"]));
}

#[test]
fn dags_that_cant_run_any_more_are_forgotten() {
    let (mut quiet, wakes, metrics) = quiet_dags();
    round(&mut quiet, at(0), &v1(&["a", "b"]), |_| RUNNING);
    // `b` is gone when visited.
    round(&mut quiet, at(1), &v1(&["a"]), |_| RUNNING);
    assert_eq!(counts(&metrics), (1, 0));
    // A gone DAG marked: visited once, not found, forgotten again.
    wakes.wake(&id("b"), WakeReason::Command);
    assert_eq!(
        round(&mut quiet, at(2), &v1(&["a"]), |_| RUNNING),
        some(&["a", "b"])
    );
    assert_eq!(counts(&metrics), (1, 0));
}

#[test]
fn off_visits_every_dag_every_round() {
    let metrics = MasterMetrics::default();
    let settings = QuietSettings {
        enabled: false,
        ..QuietSettings::default()
    };
    let mut quiet = QuietDags::new(settings, Wakes::default());
    quiet.set_metrics(metrics.clone());
    let dags = v1(&["a", "b"]);
    for second in [0, 1, 60, 61, 200] {
        assert_eq!(round(&mut quiet, at(second), &dags, |_| IDLE), Visit::All);
    }
    assert_eq!(counts(&metrics), (2, 0));
}

#[test]
fn quiet_dags_arriving_all_the_time_never_hold_back_those_waiting() {
    let (mut quiet, wakes, metrics) = quiet_dags();
    let leak = |name: String| -> &'static str { Box::leak(name.into_boxed_str()) };
    let mut dags: Vec<(&'static str, &'static str)> =
        (0..120).map(|n| (leak(format!("d{n:03}")), "v1")).collect();
    round(&mut quiet, at(0), &dags, |_| IDLE);
    round(&mut quiet, at(60), &dags, |_| IDLE);
    // From now on 4 new DAGs a second, each quiet 60 s after it arrived: the quiet ones grow
    // from 120 to 600 meanwhile, all of them joining behind the 120.
    let mut sweeps: HashMap<String, Vec<i64>> = HashMap::new();
    for second in 61..=240 {
        for k in 0..4 {
            let name = leak(format!("n{second:03}_{k}"));
            wakes.wake(&id(name), WakeReason::NewVersion);
            dags.push((name, "v1"));
        }
        for name in names(&round(&mut quiet, at(second), &dags, |_| IDLE)) {
            if name.starts_with('d') {
                sweeps.entry(name).or_default().push(second);
            }
        }
    }
    assert!(counts(&metrics).1 > 500, "{:?}", counts(&metrics));
    // Each of the 120 swept within 60 s of its last visit, every time.
    for n in 0..120 {
        let name = format!("d{n:03}");
        let times = &sweeps[&name];
        let mut last = 60;
        for &time in times {
            assert!(time - last <= 61, "{name}: swept at {times:?}");
            last = time;
        }
        assert!(240 - last <= 61, "{name}: swept at {times:?}");
    }
}

#[test]
fn a_failed_round_changes_nothing_and_its_marks_come_back() {
    let (mut quiet, wakes, metrics) = quiet_dags();
    let dags = v1(&["a", "b", "c"]);
    // A term's first round fails (the database): the next one visits every DAG again.
    assert_eq!(quiet.round(at(0)).visit, Visit::All);
    quiet.round_failed();
    assert_eq!(round(&mut quiet, at(1), &dags, |_| IDLE), Visit::All);
    assert_eq!(counts(&metrics), (0, 3));
    // An interval later the sweep (and the ID check) has just covered every DAG.
    round(&mut quiet, at(61), &dags, |_| IDLE);
    // A round with `c` marked fails: the next visits `c`, by its mark put back (the sweep
    // reaches nothing so soon).
    wakes.wake(&id("c"), WakeReason::Command);
    quiet.round(at(62));
    quiet.round_failed();
    assert_eq!(round(&mut quiet, at(63), &dags, |_| IDLE), some(&["c"]));
    // A round with the ID check due fails: the next still checks.
    assert!(quiet.round(at(121)).check);
    quiet.round_failed();
    assert!(quiet.round(at(122)).check);
}

#[test]
fn after_the_clock_steps_back_the_sweep_still_reaches_every_quiet_dag() {
    let (mut quiet, _, metrics) = quiet_dags();
    let dags = v1(&["a", "b", "c"]);
    let paused = |_: &str| Found {
        paused: true,
        ..IDLE
    };
    round(&mut quiet, at(0), &dags, paused);
    round(&mut quiet, at(1000), &dags, paused);
    assert_eq!(counts(&metrics), (0, 3));
    // The clock steps back 100 s: each of them is still swept within the interval.
    let mut first: HashMap<String, i64> = HashMap::new();
    for second in 900..=980 {
        for name in names(&round(&mut quiet, at(second), &dags, paused)) {
            first.entry(name).or_insert(second);
        }
    }
    for name in ["a", "b", "c"] {
        let at = first.get(name).copied();
        assert!(
            at.is_some_and(|at| at <= 961),
            "{name} first swept at {at:?}: {first:?}"
        );
    }
}
