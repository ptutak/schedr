use std::time::Duration;

use tokio::time::Instant;

use super::*;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn settings() -> SlowEqualsSettings {
    SlowEqualsSettings {
        factor: 3,
        floor: ms(1000),
        stale_after: Duration::from_secs(30),
    }
}

#[test]
fn it_is_the_median_of_every_workers_head_age_times_the_factor() {
    let slow = SlowEquals::new(settings());
    let now = Instant::now();
    slow.heard("w1", Some(ms(400)), now);
    slow.heard("w2", Some(ms(600)), now);
    // 400, 600, 2000: the median 600, times 3.
    assert_eq!(slow.heard("w3", Some(ms(2000)), now), ms(1800));
    // Again from w1: still one age each.
    assert_eq!(slow.heard("w1", Some(ms(800)), now), ms(2400));
}

#[test]
fn empty_queues_count_and_pull_it_to_the_floor() {
    // At a burst's end idle workers report 0: one queue still waiting is slower than that.
    let slow = SlowEquals::new(settings());
    let now = Instant::now();
    slow.heard("idle-1", Some(Duration::ZERO), now);
    slow.heard("idle-2", Some(Duration::ZERO), now);
    assert_eq!(slow.heard("busy", Some(ms(5000)), now), ms(1000));
}

#[test]
fn an_even_count_takes_the_middle_two() {
    let slow = SlowEquals::new(settings());
    let now = Instant::now();
    slow.heard("w1", Some(ms(1000)), now);
    assert_eq!(slow.heard("w2", Some(ms(2000)), now), ms(4500));
}

#[test]
fn a_worker_not_heard_for_a_while_no_longer_counts() {
    let slow = SlowEquals::new(settings());
    let start = Instant::now();
    slow.heard("gone", Some(ms(9000)), start);
    let later = start + Duration::from_secs(31);
    assert_eq!(slow.heard("w1", Some(ms(500)), later), ms(1500));
}

#[test]
fn the_median_of_nothing_is_zero() {
    assert_eq!(median(&[]), Duration::ZERO);
    assert_eq!(median(&[ms(7)]), ms(7));
}

#[test]
fn a_worker_without_a_queue_doesnt_count() {
    // Its 0 says nothing of how busy it is: counted, half the cluster without a queue would hold
    // the slow-equals at the floor (a side note on M5.2e).
    let slow = SlowEquals::new(settings());
    let now = Instant::now();
    slow.heard("plain-1", None, now);
    slow.heard("plain-2", None, now);
    slow.heard("w1", Some(ms(2000)), now);
    assert_eq!(slow.heard("w2", Some(ms(2000)), now), ms(6000));
    assert_eq!(slow.heard("plain-1", None, now), ms(6000));
}

#[test]
fn a_worker_restarted_without_a_queue_forgets_its_age() {
    let slow = SlowEquals::new(settings());
    let now = Instant::now();
    slow.heard("w1", Some(Duration::from_secs(100)), now);
    assert_eq!(slow.heard("w1", None, now), ms(1000));
}
