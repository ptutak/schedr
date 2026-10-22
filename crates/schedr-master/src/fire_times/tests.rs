use pretty_assertions::assert_eq;
use proptest::prelude::*;
use rstest::rstest;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn cron(expression: &str, timezone: &str) -> FireTimes {
    FireTimes::new(
        &Trigger::Cron {
            expression: expression.to_owned(),
            timezone: timezone.to_owned(),
            catchup: false,
        },
        None,
    )
    .unwrap()
}

fn warsaw(expression: &str) -> FireTimes {
    cron(expression, "Europe/Warsaw")
}

/// The next `n` fire times after `from`, as text.
fn forward(times: &FireTimes, from: &str, n: usize) -> Vec<String> {
    let mut t = ts(from);
    let mut found = Vec::new();
    for _ in 0..n {
        t = times.next_after(t).unwrap();
        found.push(t.to_string());
    }
    found
}

/// The previous `n` fire times before `from`, newest first, as text.
fn backward(times: &FireTimes, from: &str, n: usize) -> Vec<String> {
    let mut t = ts(from);
    let mut found = Vec::new();
    for _ in 0..n {
        t = times.previous_before(t).unwrap();
        found.push(t.to_string());
    }
    found
}

#[test]
fn a_daily_cron_in_utc() {
    let daily = cron("0 3 * * *", "UTC");
    assert_eq!(
        forward(&daily, "2026-10-15T03:00:00Z", 2),
        ["2026-10-16T03:00:00Z", "2026-10-17T03:00:00Z"]
    );
    assert_eq!(
        backward(&daily, "2026-10-15T03:00:00Z", 2),
        ["2026-10-14T03:00:00Z", "2026-10-13T03:00:00Z"]
    );
}

#[test]
fn a_cron_in_a_zone_fires_at_local_times() {
    let daily = warsaw("0 3 * * *");
    assert_eq!(
        forward(&daily, "2026-07-01T00:00:00Z", 1),
        ["2026-07-01T01:00:00Z"]
    );
    assert_eq!(
        forward(&daily, "2026-12-01T00:00:00Z", 1),
        ["2026-12-01T02:00:00Z"]
    );
}

#[test]
fn a_fixed_time_in_the_spring_gap_moves_to_the_end_of_the_gap() {
    // 2026-03-29: 02:00 CET jumps to 03:00 CEST (01:00Z); 02:30 doesn't exist.
    assert_eq!(
        forward(&warsaw("30 2 * * *"), "2026-03-28T00:00:00Z", 3),
        [
            "2026-03-28T01:30:00Z",
            "2026-03-29T01:00:00Z",
            "2026-03-30T00:30:00Z"
        ]
    );
}

#[test]
fn several_fire_times_in_the_gap_collapse_into_one() {
    assert_eq!(
        forward(&warsaw("*/15 2 * * *"), "2026-03-28T01:40:00Z", 3),
        [
            "2026-03-28T01:45:00Z",
            "2026-03-29T01:00:00Z",
            "2026-03-30T00:00:00Z"
        ]
    );
    // Wildcard hours: 02:00 and 02:30 collapse into 03:00 CEST, itself a fire time.
    assert_eq!(
        forward(&warsaw("*/30 * * * *"), "2026-03-29T00:00:00Z", 3),
        [
            "2026-03-29T00:30:00Z",
            "2026-03-29T01:00:00Z",
            "2026-03-29T01:30:00Z"
        ]
    );
}

#[test]
fn a_fixed_hour_in_the_autumn_overlap_fires_once_at_the_first_occurrence() {
    // 2026-10-25: 03:00 CEST goes back to 02:00 CET (01:00Z); 02:30 happens twice.
    assert_eq!(
        forward(&warsaw("30 2 * * *"), "2026-10-24T00:00:00Z", 3),
        [
            "2026-10-24T00:30:00Z",
            "2026-10-25T00:30:00Z",
            "2026-10-26T01:30:00Z"
        ]
    );
    assert_eq!(
        forward(&warsaw("@daily"), "2026-10-24T12:00:00Z", 1),
        ["2026-10-24T22:00:00Z"]
    );
}

#[test]
fn a_wildcard_hour_in_the_overlap_fires_at_each_occurrence() {
    assert_eq!(
        forward(&warsaw("*/30 * * * *"), "2026-10-24T23:45:00Z", 5),
        [
            "2026-10-25T00:00:00Z",
            "2026-10-25T00:30:00Z",
            "2026-10-25T01:00:00Z",
            "2026-10-25T01:30:00Z",
            "2026-10-25T02:00:00Z"
        ]
    );
    assert_eq!(
        forward(&warsaw("@hourly"), "2026-10-24T23:30:00Z", 3),
        [
            "2026-10-25T00:00:00Z",
            "2026-10-25T01:00:00Z",
            "2026-10-25T02:00:00Z"
        ]
    );
    assert_eq!(
        backward(&warsaw("*/30 * * * *"), "2026-10-25T02:00:00Z", 4),
        [
            "2026-10-25T01:30:00Z",
            "2026-10-25T01:00:00Z",
            "2026-10-25T00:30:00Z",
            "2026-10-25T00:00:00Z"
        ]
    );
}

#[test]
fn going_back_follows_the_same_rules() {
    assert_eq!(
        backward(&warsaw("30 2 * * *"), "2026-03-30T00:30:00Z", 2),
        ["2026-03-29T01:00:00Z", "2026-03-28T01:30:00Z"]
    );
    assert_eq!(
        backward(&warsaw("30 2 * * *"), "2026-10-26T01:30:00Z", 2),
        ["2026-10-25T00:30:00Z", "2026-10-24T00:30:00Z"]
    );
}

#[test]
fn six_fields_start_with_seconds() {
    assert_eq!(
        forward(&cron("30 0 3 * * *", "UTC"), "2026-10-15T00:00:00Z", 1),
        ["2026-10-15T03:00:30Z"]
    );
}

#[test]
fn every_counts_from_start_or_the_epoch() {
    let hourly = Trigger::Every {
        interval_ms: 3_600_000,
        catchup: false,
    };
    let from_epoch = FireTimes::new(&hourly, None).unwrap();
    assert_eq!(
        forward(&from_epoch, "2026-10-15T03:20:00Z", 2),
        ["2026-10-15T04:00:00Z", "2026-10-15T05:00:00Z"]
    );
    let from_start = FireTimes::new(&hourly, Some(ts("2026-10-01T00:15:00Z"))).unwrap();
    assert_eq!(
        forward(&from_start, "2026-10-15T03:15:00Z", 1),
        ["2026-10-15T04:15:00Z"]
    );
    assert_eq!(
        backward(&from_start, "2026-10-15T03:15:00Z", 1),
        ["2026-10-15T02:15:00Z"]
    );
    // Before the anchor too: fire times before `start` are fire times of the schedule.
    assert_eq!(
        backward(&from_start, "2026-10-01T00:15:00Z", 1),
        ["2026-09-30T23:15:00Z"]
    );
}

#[test]
fn every_ignores_daylight_saving() {
    let six_hours = FireTimes::new(
        &Trigger::Every {
            interval_ms: 6 * 3_600_000,
            catchup: false,
        },
        None,
    )
    .unwrap();
    assert_eq!(
        forward(&six_hours, "2026-10-24T23:00:00Z", 2),
        ["2026-10-25T00:00:00Z", "2026-10-25T06:00:00Z"]
    );
}

#[test]
fn once_has_one_fire_time_and_none_has_none() {
    let at = ts("2026-12-24T22:00:00Z");
    let once = FireTimes::new(&Trigger::Once { at }, None).unwrap();
    assert_eq!(once.next_after(ts("2026-01-01T00:00:00Z")), Some(at));
    assert_eq!(once.next_after(at), None);
    assert_eq!(once.previous_before(ts("2027-01-01T00:00:00Z")), Some(at));
    assert_eq!(once.previous_before(at), None);
    let none = FireTimes::new(&Trigger::None, None).unwrap();
    assert_eq!(none.next_after(at), None);
    assert_eq!(none.previous_before(at), None);
    assert!(!none.is_fire_time(at));
}

#[rstest]
#[case("2026-10-15T03:00:00Z", true)]
#[case("2026-10-15T03:00:01Z", false)]
fn fire_times_can_be_recognised(#[case] t: &str, #[case] expected: bool) {
    let daily = cron("0 3 * * *", "UTC");
    assert_eq!(daily.is_fire_time(ts(t)), expected);
}

#[test]
fn at_or_after_and_at_or_before_include_the_moment() {
    let daily = cron("0 3 * * *", "UTC");
    let fire = ts("2026-10-15T03:00:00Z");
    let between = ts("2026-10-15T12:00:00Z");
    assert_eq!(daily.next_at_or_after(fire), Some(fire));
    assert_eq!(daily.last_at_or_before(fire), Some(fire));
    assert_eq!(
        daily.next_at_or_after(between),
        Some(ts("2026-10-16T03:00:00Z"))
    );
    assert_eq!(daily.last_at_or_before(between), Some(fire));
}

#[rstest]
#[case("0 3 * *", "UTC", "cron expression `0 3 * *`: ")]
#[case("61 3 * * *", "UTC", "cron expression `61 3 * * *`: ")]
#[case("0 3 * * *", "Mars/Base", "unknown time zone `Mars/Base`")]
fn bad_crons_are_errors(#[case] expression: &str, #[case] tz: &str, #[case] message: &str) {
    let trigger = Trigger::Cron {
        expression: expression.to_owned(),
        timezone: tz.to_owned(),
        catchup: false,
    };
    let error = FireTimes::new(&trigger, None).unwrap_err();
    assert!(error.to_string().starts_with(message), "{error}");
}

#[test]
fn a_zero_interval_is_an_error() {
    let error = FireTimes::new(
        &Trigger::Every {
            interval_ms: 0,
            catchup: false,
        },
        None,
    )
    .unwrap_err();
    assert_eq!(error, FireTimesError::ZeroInterval);
    assert_eq!(
        error.to_string(),
        "the interval of an Every trigger must be positive"
    );
}

#[test]
fn a_cron_that_never_fires_has_no_fire_times() {
    let never = cron("0 0 30 2 *", "UTC");
    assert_eq!(never.next_after(ts("2026-01-01T00:00:00Z")), None);
    assert_eq!(never.previous_before(ts("2026-01-01T00:00:00Z")), None);
}

#[rstest]
#[case("30 2 * * *", true)]
#[case("0 0 2 * * *", true)]
#[case("@daily", true)]
#[case("@hourly", false)]
#[case("*/15 * * * *", false)]
#[case("0 1-3 * * *", false)]
#[case("0 1,2 * * *", false)]
fn the_hour_field_decides_the_overlap_rule(#[case] expression: &str, #[case] fixed: bool) {
    assert_eq!(hour_field_is_fixed(expression), fixed);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Around both 2026 changes in Warsaw, fire times strictly increase and going back from
    /// each one gives the one before.
    #[test]
    fn fire_times_are_ordered_and_consistent(
        expression in prop::sample::select(vec![
            "30 2 * * *", "0 3 * * *", "*/15 * * * *", "*/20 2 * * *", "0 * * * *",
            "45 1-3 * * *", "@daily", "0 0 2 * * *",
        ]),
        offset_minutes in 0i64..(3 * 24 * 60),
        autumn in any::<bool>(),
    ) {
        let times = warsaw(expression);
        let base = if autumn { ts("2026-10-23T12:00:00Z") } else { ts("2026-03-27T12:00:00Z") };
        let mut t = base.checked_add(SignedDuration::from_mins(offset_minutes)).unwrap();
        let mut previous: Option<Timestamp> = None;
        for _ in 0..12 {
            let next = times.next_after(t).unwrap();
            prop_assert!(next > t);
            if let Some(p) = previous {
                prop_assert_eq!(times.previous_before(next), Some(p));
            }
            prop_assert!(times.is_fire_time(next));
            previous = Some(next);
            t = next;
        }
    }
}
