use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use tokio::sync::watch;

use super::*;

/// A fake process: it ends when `end` is sent (or on a signal it reacts to).
struct Fake {
    ended: watch::Receiver<Option<Termination>>,
    on_term: Option<Termination>,
    signals: Arc<Mutex<Vec<Signal>>>,
    end: watch::Sender<Option<Termination>>,
    fail_signal: bool,
    /// Something of its group left after it ended, until SIGKILL.
    group: Arc<AtomicBool>,
}

impl Fake {
    fn new(on_term: Option<Termination>) -> Self {
        let (end, ended) = watch::channel(None);
        Self {
            ended,
            on_term,
            signals: Arc::default(),
            end,
            fail_signal: false,
            group: Arc::default(),
        }
    }
}

impl Process for Fake {
    async fn wait(&mut self) -> io::Result<Termination> {
        let mut ended = self.ended.clone();
        let value = ended.wait_for(Option::is_some).await.expect("sender alive");
        Ok(value.expect("checked"))
    }

    fn signal(&mut self, signal: Signal) -> io::Result<()> {
        if self.fail_signal {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied));
        }
        self.signals.lock().unwrap().push(signal);
        match signal {
            Signal::Term => {
                if let Some(termination) = self.on_term {
                    self.end.send_replace(Some(termination));
                }
            }
            Signal::Kill => {
                self.end.send_replace(Some(Termination::Signaled(9)));
                self.group.store(false, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    fn group_alive(&self) -> bool {
        self.group.load(Ordering::SeqCst)
    }
}

const LIMITS: Limits = Limits {
    timeout: Some(Duration::from_secs(60)),
    grace: Duration::from_secs(30),
    interrupt_grace: Duration::from_secs(3),
};

/// No reason to stop, ever.
fn never() -> Stops {
    Stops::new().1
}

/// `reason`, sent `secs` seconds from now.
fn stopping_after(secs: u64, reason: StopReason) -> Stops {
    let (sender, stops) = Stops::new();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        let _ = sender.send(reason);
    });
    stops
}

#[tokio::test(start_paused = true)]
async fn a_process_that_ends_by_itself_is_not_stopped() {
    let mut fake = Fake::new(None);
    let end = fake.end.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        end.send_replace(Some(Termination::Exited(0)));
    });
    let result = supervise(&mut fake, LIMITS, never()).await.unwrap();
    assert_eq!(
        result,
        Supervised {
            termination: Termination::Exited(0),
            stopped: None,
            killed: false
        }
    );
    assert_eq!(*fake.signals.lock().unwrap(), Vec::<Signal>::new());
}

#[tokio::test(start_paused = true)]
async fn a_timeout_sends_sigterm_and_a_graceful_exit_counts() {
    let mut fake = Fake::new(Some(Termination::Exited(0)));
    let started = tokio::time::Instant::now();
    let result = supervise(&mut fake, LIMITS, never()).await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(60));
    assert_eq!(
        result,
        Supervised {
            termination: Termination::Exited(0),
            stopped: Some(StopReason::TimedOut),
            killed: false
        }
    );
    // Nothing left of its group: no SIGKILL (decision 100).
    assert_eq!(*fake.signals.lock().unwrap(), vec![Signal::Term]);
}

#[tokio::test(start_paused = true)]
async fn a_process_ignoring_sigterm_gets_sigkill_after_the_grace_time() {
    let mut fake = Fake::new(None);
    let started = tokio::time::Instant::now();
    let result = supervise(&mut fake, LIMITS, never()).await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(90));
    assert_eq!(
        result,
        Supervised {
            termination: Termination::Signaled(9),
            stopped: Some(StopReason::TimedOut),
            killed: true
        }
    );
    assert_eq!(
        *fake.signals.lock().unwrap(),
        vec![Signal::Term, Signal::Kill]
    );
}

#[tokio::test(start_paused = true)]
async fn an_interrupted_process_ignoring_sigterm_gets_sigkill_after_the_interrupt_grace() {
    // B8: a drain's interrupt must end before the pod is killed, not after the cancel grace.
    let mut fake = Fake::new(None);
    let started = tokio::time::Instant::now();
    let result = supervise(
        &mut fake,
        LIMITS,
        stopping_after(0, StopReason::Interrupted),
    )
    .await
    .unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(3));
    assert_eq!(result.stopped, Some(StopReason::Interrupted));
    assert!(result.killed);
    assert_eq!(
        *fake.signals.lock().unwrap(),
        vec![Signal::Term, Signal::Kill]
    );
}

#[tokio::test(start_paused = true)]
async fn cancelling_stops_the_process_with_the_given_reason() {
    let mut fake = Fake::new(Some(Termination::Signaled(15)));
    let cancel = stopping_after(2, StopReason::Cancelled);
    let result = supervise(&mut fake, LIMITS, cancel).await.unwrap();
    assert_eq!(result.stopped, Some(StopReason::Cancelled));
    assert_eq!(result.termination, Termination::Signaled(15));
}

#[tokio::test(start_paused = true)]
async fn a_process_over_its_memory_budget_is_killed_at_once() {
    let mut fake = Fake::new(Some(Termination::Exited(0)));
    let started = tokio::time::Instant::now();
    let result = supervise(&mut fake, LIMITS, stopping_after(0, StopReason::OomKilled))
        .await
        .unwrap();
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(
        result,
        Supervised {
            termination: Termination::Signaled(9),
            stopped: Some(StopReason::OomKilled),
            killed: true
        }
    );
    assert_eq!(*fake.signals.lock().unwrap(), vec![Signal::Kill]);
}

#[tokio::test(start_paused = true)]
async fn without_a_timeout_only_a_stop_ends_supervision() {
    let mut fake = Fake::new(Some(Termination::Exited(1)));
    let limits = Limits {
        timeout: None,
        grace: Duration::from_secs(1),
        interrupt_grace: Duration::from_secs(1),
    };
    let drain = stopping_after(3600, StopReason::Interrupted);
    let result = supervise(&mut fake, limits, drain).await.unwrap();
    assert_eq!(result.stopped, Some(StopReason::Interrupted));
}

#[tokio::test(start_paused = true)]
async fn a_failing_signal_is_reported() {
    let mut fake = Fake::new(None);
    fake.fail_signal = true;
    let err = supervise(&mut fake, LIMITS, never()).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
}

#[test]
fn signals_compare() {
    assert_ne!(Signal::Term, Signal::Kill);
}

/// Supervises a process ignoring SIGTERM, sending it `reasons`, each at its time.
async fn stopped_with(reasons: &[(u64, StopReason)]) -> (Duration, Supervised) {
    let mut fake = Fake::new(None);
    let (sender, stops) = Stops::new();
    for &(at, reason) in reasons {
        let sender = sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(at)).await;
            let _ = sender.send(reason);
        });
    }
    let started = tokio::time::Instant::now();
    let supervised = supervise(&mut fake, LIMITS, stops).await.unwrap();
    (started.elapsed(), supervised)
}

#[tokio::test(start_paused = true)]
async fn a_drains_interrupt_during_a_cancels_grace_hurries_the_sigkill() {
    // B9: cancelled at 0 (SIGKILL due at 30 s), the drain's time up at 10 s: SIGKILL at 13 s.
    let (took, supervised) =
        stopped_with(&[(0, StopReason::Cancelled), (10, StopReason::Interrupted)]).await;
    assert_eq!(took, Duration::from_secs(13));
    assert_eq!(supervised.stopped, Some(StopReason::Cancelled));
    assert!(supervised.killed);
}

#[tokio::test(start_paused = true)]
async fn a_late_interrupt_never_puts_the_sigkill_off() {
    let (took, _) =
        stopped_with(&[(0, StopReason::Cancelled), (29, StopReason::Interrupted)]).await;
    assert_eq!(took, Duration::from_secs(30));
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_process_is_hurried_by_an_interrupt_too() {
    // The timeout (60 s) sends SIGTERM; the drain's interrupt at 70 s: SIGKILL at 73 s.
    let (took, supervised) = stopped_with(&[(70, StopReason::Interrupted)]).await;
    assert_eq!(took, Duration::from_secs(73));
    assert_eq!(supervised.stopped, Some(StopReason::TimedOut));
}

#[tokio::test(start_paused = true)]
async fn the_memory_watchdog_during_a_grace_kills_at_once() {
    let (took, supervised) =
        stopped_with(&[(0, StopReason::Cancelled), (5, StopReason::OomKilled)]).await;
    assert_eq!(took, Duration::from_secs(5));
    assert_eq!(supervised.stopped, Some(StopReason::Cancelled));
}

#[tokio::test(start_paused = true)]
async fn a_repeated_cancel_changes_nothing() {
    let (took, _) = stopped_with(&[(0, StopReason::Cancelled), (5, StopReason::Cancelled)]).await;
    assert_eq!(took, Duration::from_secs(30));
}

#[tokio::test]
async fn stops_come_in_order_from_every_sender() {
    let (sender, mut stops) = Stops::new();
    sender.send(StopReason::Cancelled).unwrap();
    stops.sender().send(StopReason::Interrupted).unwrap();
    assert_eq!(stops.next_reason().await, StopReason::Cancelled);
    assert_eq!(stops.next_reason().await, StopReason::Interrupted);
    // Every sender gone but its own: it waits rather than ending.
    drop(sender);
    let more = tokio::time::timeout(Duration::from_millis(20), stops.next_reason()).await;
    assert!(more.is_err());
}

#[tokio::test(start_paused = true)]
async fn what_is_left_of_the_group_keeps_the_grace_then_is_killed() {
    let mut fake = Fake::new(Some(Termination::Exited(0)));
    fake.group.store(true, Ordering::SeqCst);
    let (sender, stops) = Stops::new();
    sender.send(StopReason::Cancelled).unwrap();
    let started = tokio::time::Instant::now();
    let result = supervise(&mut fake, LIMITS, stops).await.unwrap();
    assert_eq!(started.elapsed(), LIMITS.grace);
    assert_eq!(result.termination, Termination::Exited(0));
    assert_eq!(
        *fake.signals.lock().unwrap(),
        vec![Signal::Term, Signal::Kill]
    );
}

/// An interrupt arriving while the rest of the group shuts down gets its whole grace from
/// when it arrives, not from the poll before it.
#[tokio::test(start_paused = true)]
async fn an_interrupt_after_the_leader_ended_gets_its_grace_from_its_arrival() {
    let mut fake = Fake::new(Some(Termination::Exited(0)));
    fake.group.store(true, Ordering::SeqCst);
    let (sender, stops) = Stops::new();
    sender.send(StopReason::Cancelled).unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = sender.send(StopReason::Interrupted);
    });
    let started = tokio::time::Instant::now();
    supervise(&mut fake, LIMITS, stops).await.unwrap();
    assert_eq!(
        started.elapsed(),
        Duration::from_millis(50) + LIMITS.interrupt_grace
    );
}
