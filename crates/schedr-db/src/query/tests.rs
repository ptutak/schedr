use pretty_assertions::assert_eq;

use super::*;

#[test]
fn times_are_stored_in_utc_with_six_fraction_digits() {
    let whole: Timestamp = "2026-10-21T10:00:00+02:00".parse().unwrap();
    assert_eq!(db_time(whole), "2026-10-21T08:00:00.000000Z");
    let fraction: Timestamp = "2026-10-21T08:00:00.25Z".parse().unwrap();
    assert_eq!(db_time(fraction), "2026-10-21T08:00:00.250000Z");
    assert!(
        db_time(fraction) > db_time(whole),
        "the text sorts in time order"
    );
}
