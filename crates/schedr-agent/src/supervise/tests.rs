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
            }
        }
        Ok(())
    }
}

const LIMITS: Limits = Limits {
    timeout: Some(Duration::from_secs(60)),
    grace: Duration::from_secs(30),
};

fn never() -> impl Future<Output = StopReason> {
    std::future::pending()
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
async fn cancelling_stops_the_process_with_the_given_reason() {
    let mut fake = Fake::new(Some(Termination::Signaled(15)));
    let cancel = async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        StopReason::Cancelled
    };
    let result = supervise(&mut fake, LIMITS, cancel).await.unwrap();
    assert_eq!(result.stopped, Some(StopReason::Cancelled));
    assert_eq!(result.termination, Termination::Signaled(15));
}

#[tokio::test(start_paused = true)]
async fn a_process_over_its_memory_budget_is_killed_at_once() {
    let mut fake = Fake::new(Some(Termination::Exited(0)));
    let started = tokio::time::Instant::now();
    let result = supervise(&mut fake, LIMITS, async { StopReason::OomKilled })
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
    };
    let drain = async {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        StopReason::Interrupted
    };
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
