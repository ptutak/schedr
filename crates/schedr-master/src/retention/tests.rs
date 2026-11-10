use pretty_assertions::assert_eq;

use super::*;

const DAY: Duration = Duration::from_hours(24);

fn section(
    runs: Age,
    outputs: Option<Age>,
    events: Option<Age>,
    commands: Age,
) -> RetentionSection {
    RetentionSection {
        runs,
        outputs,
        events,
        commands,
    }
}

#[test]
fn outputs_kept_as_long_as_their_runs_go_with_them() {
    let hour = Duration::from_hours(1);
    let defaults = RetentionSettings::from_config(&RetentionSection::default(), hour, 100);
    assert_eq!(
        (
            defaults.runs,
            defaults.outputs,
            defaults.events,
            defaults.commands
        ),
        (Some(90 * DAY), None, Some(90 * DAY), Some(365 * DAY))
    );
    let shorter = section(
        Age::For(30 * DAY),
        Some(Age::For(7 * DAY)),
        None,
        Age::Forever,
    );
    let set = RetentionSettings::from_config(&shorter, hour, 0);
    assert_eq!(
        (set.outputs, set.events, set.commands),
        (Some(7 * DAY), Some(30 * DAY), None)
    );
    assert_eq!(set.batch, 1, "at least one a transaction");
    let longer = section(
        Age::For(30 * DAY),
        Some(Age::For(60 * DAY)),
        None,
        Age::Forever,
    );
    assert_eq!(
        RetentionSettings::from_config(&longer, hour, 100).outputs,
        None
    );
    // Runs kept forever: their outputs may still go.
    let forever = section(Age::Forever, Some(Age::For(7 * DAY)), None, Age::Forever);
    let set = RetentionSettings::from_config(&forever, hour, 100);
    assert_eq!(
        (set.runs, set.outputs, set.events),
        (None, Some(7 * DAY), None)
    );
}

#[test]
fn an_age_beyond_the_calendar_keeps_everything() {
    let now: Timestamp = "2026-11-10T00:00:00Z".parse().unwrap();
    assert_eq!(cut(now, Duration::from_secs(u64::MAX)), Timestamp::MIN);
    assert_eq!(cut(now, DAY), "2026-11-09T00:00:00Z".parse().unwrap());
}

#[tokio::test]
async fn a_pass_stops_once_this_master_no_longer_leads() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let settings =
        RetentionSettings::from_config(&RetentionSection::default(), Duration::from_hours(1), 100);
    let now = "2026-11-10T00:00:00Z".parse().unwrap();
    let metrics = MasterMetrics::default();
    let calls = std::cell::Cell::new(0);
    let leading = || {
        calls.set(calls.get() + 1);
        false
    };
    assert_eq!(
        pass(&db, &settings, now, leading, &metrics).await.unwrap(),
        Pass::default()
    );
    // Asked before each kind's first batch, and before the removed DAGs: nothing deleted.
    assert_eq!(calls.get(), 5);
    assert_eq!(
        pass(&db, &settings, now, || true, &metrics).await.unwrap(),
        Pass::default()
    );
}
