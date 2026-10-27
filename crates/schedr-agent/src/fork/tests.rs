use std::time::Duration;

use pretty_assertions::assert_eq;

use tokio::sync::watch;

use super::*;

#[test]
fn the_backoff_doubles_up_to_its_cap() {
    let mut backoff = BACKOFF_FIRST / 2;
    let mut waits = Vec::new();
    for _ in 0..12 {
        backoff = next_backoff(backoff, None);
        waits.push(backoff.as_secs());
    }
    assert_eq!(waits, [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300, 300]);
}

#[test]
fn a_server_up_long_enough_starts_the_backoff_again() {
    let long = Some(BACKOFF_RESET_AFTER);
    let short = BACKOFF_RESET_AFTER.checked_sub(Duration::from_secs(1));
    assert_eq!(next_backoff(BACKOFF_MAX, long), BACKOFF_FIRST);
    assert_eq!(
        next_backoff(Duration::from_secs(8), short),
        Duration::from_secs(16)
    );
}

#[test]
fn kinds_and_fallbacks_have_their_metric_labels() {
    assert_eq!(
        [Kind::Preloaded.label(), Kind::Bare.label()],
        ["preloaded", "bare"]
    );
    assert_eq!(
        [
            Fallback::Disabled,
            Fallback::Down,
            Fallback::ForkFailed,
            Fallback::ShimDied
        ]
        .map(Fallback::label),
        ["disabled", "down", "fork_failed", "shim_died"]
    );
}

#[test]
fn only_a_failing_import_is_retried_and_within_its_window() {
    let window = Duration::from_mins(20);
    let just_under = window.checked_sub(Duration::from_secs(1)).unwrap();
    assert!(retried(Cause::Import, Duration::ZERO, window));
    assert!(retried(Cause::Import, just_under, window));
    assert!(!retried(Cause::Import, window, window));
    assert!(!retried(Cause::Lasting, Duration::ZERO, window));
    // `0s`: never retried.
    assert!(!retried(Cause::Import, Duration::ZERO, Duration::ZERO));
}

#[test]
fn a_warning_says_what_a_disabled_server_means_for_tasks() {
    let (warnings, watching) = watch::channel(Vec::new());
    let board = WarningBoard {
        interpreter: "python".to_owned(),
        bare_alone: false,
        reasons: std::sync::Mutex::default(),
        warnings,
    };
    let messages = || -> Vec<String> {
        watching
            .borrow()
            .iter()
            .map(|w| w.message.clone())
            .collect()
    };
    board.record(Kind::Preloaded, Some("threads"));
    assert_eq!(
        messages(),
        [
            "the preloaded fork server is disabled: threads; tasks are forked by the bare fork \
          server, or spawned"
        ]
    );
    // Both disabled (e.g. a version mismatch): neither forks, every task is spawned.
    board.record(Kind::Bare, Some("version"));
    assert_eq!(
        messages(),
        [
            "the preloaded fork server is disabled: threads; tasks are spawned",
            "the bare fork server is disabled: version; tasks are spawned",
        ]
    );
    board.record(Kind::Preloaded, None);
    assert_eq!(
        messages(),
        [
            "the bare fork server is disabled: version; tasks the preloaded fork server can't \
          fork are spawned"
        ]
    );
    board.record(Kind::Bare, None);
    assert_eq!(messages(), Vec::<String>::new());
}
