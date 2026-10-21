use std::time::Duration;

use pretty_assertions::assert_eq;
use rstest::rstest;
use serde::Deserialize;

use super::*;

#[rstest]
#[case("500ms", 500)]
#[case("30s", 30_000)]
#[case("5m", 300_000)]
#[case("1h30m", 5_400_000)]
#[case("2d", 172_800_000)]
#[case("1d2h3m4s5ms", 93_784_005)]
#[case("0s", 0)]
fn parses_durations(#[case] s: &str, #[case] ms: u64) {
    assert_eq!(parse_duration(s), Ok(Duration::from_millis(ms)));
}

#[rstest]
#[case("")]
#[case("30")]
#[case("s")]
#[case("30 s")]
#[case("1.5h")]
#[case("30sec")]
#[case("30m1h")]
#[case("1m1m")]
#[case("-5s")]
#[case("99999999999999999999s")]
#[case("999999999999999d")]
#[case("18446744073709551s18446744073709551ms")]
fn rejects_malformed_durations(#[case] s: &str) {
    assert_eq!(parse_duration(s), Err(UnitError::Duration(s.to_owned())));
}

#[rstest]
#[case("512B", 512)]
#[case("64KiB", 65_536)]
#[case("4MiB", 4_194_304)]
#[case("1GiB", 1_073_741_824)]
#[case("0B", 0)]
fn parses_sizes(#[case] s: &str, #[case] bytes: u64) {
    assert_eq!(parse_size(s), Ok(bytes));
}

#[rstest]
#[case("")]
#[case("4")]
#[case("MiB")]
#[case("4MB")]
#[case("4 MiB")]
#[case("4mib")]
#[case("99999999999999999999B")]
#[case("99999999999GiB")]
fn rejects_malformed_sizes(#[case] s: &str) {
    assert_eq!(parse_size(s), Err(UnitError::Size(s.to_owned())));
}

#[test]
fn error_messages_show_examples() {
    assert_eq!(
        UnitError::Duration("x".into()).to_string(),
        "`x` is not a duration (use e.g. `500ms`, `30s`, `5m`, `1h30m`, `2d`)"
    );
    assert_eq!(
        UnitError::Size("x".into()).to_string(),
        "`x` is not a size (use e.g. `512B`, `64KiB`, `4MiB`, `1GiB`)"
    );
}

#[derive(Debug, Deserialize)]
struct WithDuration {
    #[serde(deserialize_with = "deserialize_duration")]
    timeout: Duration,
}

#[test]
fn deserialize_duration_reads_strings() {
    let parsed: WithDuration = figment::Figment::from(figment::providers::Serialized::defaults(
        std::collections::BTreeMap::from([("timeout", "1h30m")]),
    ))
    .extract()
    .unwrap();
    assert_eq!(parsed.timeout, Duration::from_mins(90));
}

#[test]
fn deserialize_duration_rejects_bad_strings_and_numbers() {
    let bad_string = figment::Figment::from(figment::providers::Serialized::defaults(
        std::collections::BTreeMap::from([("timeout", "soon")]),
    ))
    .extract::<WithDuration>()
    .unwrap_err();
    assert!(
        bad_string.to_string().contains("`soon` is not a duration"),
        "{bad_string}"
    );

    let number = figment::Figment::from(figment::providers::Serialized::defaults(
        std::collections::BTreeMap::from([("timeout", 30)]),
    ))
    .extract::<WithDuration>()
    .unwrap_err();
    assert!(
        number
            .to_string()
            .contains("a duration string such as `30s` or `1h30m`"),
        "{number}"
    );
}
