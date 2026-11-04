use pretty_assertions::assert_eq;

use super::*;

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

/// The default split (3 s to SIGKILL, 2 s to report) of a `grace` long drain.
fn periods(grace: u64) -> DrainPeriods {
    DrainPeriods {
        grace: secs(grace),
        ..DrainPeriods::default()
    }
}

#[test]
fn running_attempts_get_the_grace_period_minus_the_terminate_and_report_periods_then_are_interrupted_once()
 {
    let start = Instant::now();
    let mut drain = Drain::new(start, periods(30));
    assert_eq!(drain.step(start + secs(24), 2, 0, true), DrainStep::Wait);
    assert_eq!(
        drain.step(start + secs(25), 2, 0, true),
        DrainStep::Interrupt
    );
    assert_eq!(drain.step(start + secs(26), 2, 0, true), DrainStep::Wait);
    assert_eq!(drain.step(start + secs(27), 0, 0, true), DrainStep::Finish);
}

#[test]
fn a_grace_period_shorter_than_the_terminate_and_report_periods_interrupts_at_once() {
    let start = Instant::now();
    let mut drain = Drain::new(start, periods(2));
    assert_eq!(drain.step(start, 1, 0, true), DrainStep::Interrupt);
}

#[test]
fn a_second_request_interrupts_now() {
    let start = Instant::now();
    let mut drain = Drain::new(start, periods(600));
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
    let mut drain = Drain::new(start, periods(30));
    assert_eq!(drain.step(start, 0, 1, true), DrainStep::Wait);
    assert_eq!(drain.step(start + secs(4), 0, 1, true), DrainStep::Wait);
    assert_eq!(drain.step(start + secs(5), 0, 1, true), DrainStep::Finish);

    let mut drain = Drain::new(start, periods(30));
    assert_eq!(drain.step(start, 0, 1, false), DrainStep::Finish);
    let mut drain = Drain::new(start, periods(30));
    assert_eq!(drain.step(start, 0, 1, true), DrainStep::Wait);
    // Something runs again (a result resent): the wait starts over.
    assert_eq!(drain.step(start + secs(4), 1, 1, true), DrainStep::Wait);
    assert_eq!(drain.step(start + secs(6), 0, 1, true), DrainStep::Wait);
}

#[test]
fn the_terminate_and_report_periods_are_configurable() {
    let start = Instant::now();
    let periods = DrainPeriods {
        grace: secs(60),
        terminate_period: secs(10),
        report_period: secs(5),
        ack_wait: ACK_WAIT,
    };
    let mut drain = Drain::new(start, periods);
    assert_eq!(drain.end(), start + secs(60));
    assert_eq!(drain.step(start + secs(44), 1, 0, true), DrainStep::Wait);
    assert_eq!(
        drain.step(start + secs(45), 1, 0, true),
        DrainStep::Interrupt
    );
    assert_eq!(DrainPeriods::default(), periods_of(30, 3, 2));
}

fn periods_of(grace: u64, terminate_period: u64, report_period: u64) -> DrainPeriods {
    DrainPeriods {
        grace: secs(grace),
        terminate_period: secs(terminate_period),
        report_period: secs(report_period),
        ack_wait: ACK_WAIT,
    }
}

#[test]
fn waiting_for_acknowledgements_never_runs_past_the_end_of_the_grace_period() {
    let start = Instant::now();
    let mut drain = Drain::new(start, periods(30));
    // Interrupted at 25 s, the results sent at 29 s: 1 s is left, not ACK_WAIT.
    assert_eq!(
        drain.step(start + secs(25), 1, 0, true),
        DrainStep::Interrupt
    );
    assert_eq!(drain.step(start + secs(29), 0, 1, true), DrainStep::Wait);
    assert_eq!(drain.step(start + secs(30), 0, 1, true), DrainStep::Finish);
}

#[test]
fn a_task_is_stopped_early_enough_for_one_try_of_its_cleanup() {
    // A grace of 100 s, less the 5 s of the terminate and report periods: 95 s.
    let start = Instant::now();
    let drain = Drain::new(start, periods_of(100, 3, 2));
    assert!(!drain.stop_due(start + secs(74), secs(20)));
    assert!(drain.stop_due(start + secs(75), secs(20)));
    assert!(!drain.stop_due(start + secs(94), Duration::ZERO));
    assert!(drain.stop_due(start, secs(1000)), "too long: at once");
    assert!(drain.stop_due(start, Duration::MAX), "no overflow");
}
