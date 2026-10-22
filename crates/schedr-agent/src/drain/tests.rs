use pretty_assertions::assert_eq;

use super::*;

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

#[test]
fn running_attempts_get_the_grace_period_minus_the_margin_then_are_interrupted_once() {
    let start = Instant::now();
    let mut drain = Drain::new(start, secs(30));
    assert_eq!(drain.step(start + secs(24), 2, 0, true), DrainStep::Wait);
    assert_eq!(
        drain.step(start + secs(25), 2, 0, true),
        DrainStep::Interrupt
    );
    assert_eq!(drain.step(start + secs(26), 2, 0, true), DrainStep::Wait);
    assert_eq!(drain.step(start + secs(27), 0, 0, true), DrainStep::Finish);
}

#[test]
fn a_grace_period_shorter_than_the_margin_interrupts_at_once() {
    let start = Instant::now();
    let mut drain = Drain::new(start, secs(2));
    assert_eq!(drain.step(start, 1, 0, true), DrainStep::Interrupt);
}

#[test]
fn a_second_request_interrupts_now() {
    let start = Instant::now();
    let mut drain = Drain::new(start, secs(600));
    drain.force(start + secs(1));
    assert_eq!(
        drain.step(start + secs(1), 1, 0, true),
        DrainStep::Interrupt
    );
    // A later deadline never comes back.
    drain.force(start + secs(500));
    assert_eq!(drain.step(start + secs(2), 1, 0, true), DrainStep::Wait);
}

#[test]
fn the_last_results_are_waited_for_a_while_unless_the_master_is_gone() {
    let start = Instant::now();
    let mut drain = Drain::new(start, secs(30));
    assert_eq!(drain.step(start, 0, 1, true), DrainStep::Wait);
    assert_eq!(drain.step(start + secs(4), 0, 1, true), DrainStep::Wait);
    assert_eq!(drain.step(start + secs(5), 0, 1, true), DrainStep::Finish);

    let mut drain = Drain::new(start, secs(30));
    assert_eq!(drain.step(start, 0, 1, false), DrainStep::Finish);
    let mut drain = Drain::new(start, secs(30));
    assert_eq!(drain.step(start, 0, 1, true), DrainStep::Wait);
    // Something runs again (a result resent): the wait starts over.
    assert_eq!(drain.step(start + secs(4), 1, 1, true), DrainStep::Wait);
    assert_eq!(drain.step(start + secs(6), 0, 1, true), DrainStep::Wait);
}
