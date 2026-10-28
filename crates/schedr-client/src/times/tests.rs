use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::json;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

#[rstest]
#[case("2026-10-15T03:00:00Z", "2026-10-15T03:00:00Z")]
#[case("2026-10-15T05:00+02:00", "2026-10-15T03:00:00Z")]
#[case("2026-10-14T23:00-04:00", "2026-10-15T03:00:00Z")]
fn a_time_with_an_offset_is_taken_as_given(#[case] text: &str, #[case] expected: &str) {
    assert_eq!(
        parse_for(text),
        Ok(ForTime {
            at: ts(expected),
            warning: None
        })
    );
}

#[rstest]
#[case("2026-10-15T03:00", "2026-10-15T03:00:00Z", "2026-10-15T03:00Z")]
#[case("2026-10-15 03:00:30", "2026-10-15T03:00:30Z", "2026-10-15T03:00:30Z")]
#[case("2026-10-15", "2026-10-15T00:00:00Z", "2026-10-15T00:00Z")]
fn a_time_without_an_offset_is_utc_with_a_warning(
    #[case] text: &str,
    #[case] expected: &str,
    #[case] shown: &str,
) {
    let parsed = parse_for(text).unwrap();
    assert_eq!(parsed.at, ts(expected));
    assert_eq!(
        parsed.warning.unwrap(),
        format!("warning: no offset given: interpreted as {shown} (UTC)")
    );
}

#[test]
fn other_text_is_an_error_with_examples() {
    assert_eq!(
        parse_for("yesterday"),
        Err("`--for yesterday` isn't a date or a date-time \
             (e.g. 2026-10-15T03:00, 2026-10-15T03:00+02:00, 2026-10-15)"
            .to_owned())
    );
}

#[test]
fn times_are_shown_in_utc_and_local() {
    let warsaw = TimeZone::get("Europe/Warsaw").unwrap();
    let new_york = TimeZone::get("America/New_York").unwrap();
    assert_eq!(
        show(ts("2026-10-15T03:00:00Z"), &warsaw),
        "2026-10-15 03:00 UTC · 05:00 CEST"
    );
    assert_eq!(
        show(ts("2026-10-15T01:00:00Z"), &new_york),
        "2026-10-15 01:00 UTC · 2026-10-14 21:00 EDT"
    );
    assert_eq!(
        show(ts("2026-12-15T03:00:15Z"), &warsaw),
        "2026-12-15 03:00:15 UTC · 04:00:15 CET"
    );
    assert_eq!(
        show(ts("2026-10-15T03:00:00Z"), &TimeZone::UTC),
        "2026-10-15 03:00 UTC"
    );
}

#[test]
fn short_times_are_utc_with_the_local_offset() {
    let warsaw = TimeZone::get("Europe/Warsaw").unwrap();
    assert_eq!(
        show_short(ts("2025-12-31T23:00:00Z"), &warsaw),
        "2025-12-31 23:00Z (local:+1h)"
    );
    assert_eq!(
        show_short(ts("2026-06-30T22:00:00Z"), &warsaw),
        "2026-06-30 22:00Z (local:+2h)"
    );
    assert_eq!(
        show_short(
            ts("2026-10-15T01:00:00Z"),
            &TimeZone::get("America/New_York").unwrap()
        ),
        "2026-10-15 01:00Z (local:-4h)"
    );
    assert_eq!(
        show_short(
            ts("2026-10-15T03:00:15Z"),
            &TimeZone::get("Asia/Kolkata").unwrap()
        ),
        "2026-10-15 03:00:15Z (local:+5h30m)"
    );
    assert_eq!(
        show_short(ts("2026-10-15T03:00:00Z"), &TimeZone::UTC),
        "2026-10-15 03:00Z"
    );
}

#[test]
fn the_local_zone_comes_from_tz_or_the_system() {
    assert_eq!(
        local_zone(Some("Europe/Warsaw")).iana_name(),
        Some("Europe/Warsaw")
    );
    assert_eq!(
        local_zone(Some(":Asia/Tokyo")).iana_name(),
        Some("Asia/Tokyo")
    );
    let fallback = local_zone(Some("Mars/Base"));
    assert_eq!(fallback, local_zone(None));
}

#[test]
fn triggers_are_written_as_people_write_them() {
    let utc = TimeZone::UTC;
    let cron = json!({"kind": "cron", "expression": "@daily", "timezone": "UTC", "catchup": false});
    assert_eq!(trigger_text(&cron, &utc), "@daily");
    assert_eq!(
        trigger_text(
            &json!({"kind": "every", "interval_ms": 5_400_000, "catchup": false}),
            &utc
        ),
        "every 1h 30m"
    );
    assert_eq!(
        trigger_text(
            &json!({"kind": "every", "interval_ms": 3_600_000, "catchup": true}),
            &utc
        ),
        "every 1h, catchup"
    );
    assert_eq!(
        trigger_text(&json!({"kind": "once", "at": "2026-12-24T22:00:00Z"}), &utc),
        "once at 2026-12-24 22:00Z"
    );
    assert_eq!(trigger_text(&json!({"kind": "none"}), &utc), "-");
}

#[test]
fn a_triggers_window_is_shown_in_utc_with_the_local_offset() {
    let warsaw = TimeZone::get("Europe/Warsaw").unwrap();
    let window = json!({
        "kind": "cron", "expression": "0 3 * * *", "timezone": "Europe/Warsaw",
        "catchup": true, "start": "2025-12-31T23:00:00Z", "end": "2026-06-30T22:00:00Z"
    });
    assert_eq!(
        trigger_text(&window, &warsaw),
        "0 3 * * * (Europe/Warsaw), catchup, \
         2025-12-31 23:00Z (local:+1h)..2026-06-30 22:00Z (local:+2h)"
    );
    let until = json!({"kind": "every", "interval_ms": 60_000, "catchup": false, "start": null,
                       "end": "2026-12-31T00:00:00Z"});
    assert_eq!(
        trigger_text(&until, &TimeZone::UTC),
        "every 1m, ..2026-12-31 00:00Z"
    );
    let from = json!({"kind": "cron", "expression": "@daily", "timezone": "UTC",
                      "catchup": false, "start": "2026-01-01T00:00:00Z", "end": null});
    assert_eq!(
        trigger_text(&from, &TimeZone::UTC),
        "@daily, 2026-01-01 00:00Z.."
    );
}
