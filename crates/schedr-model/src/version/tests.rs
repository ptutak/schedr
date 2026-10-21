use pretty_assertions::assert_eq;
use rstest::rstest;
use schemars::schema_for;

use super::*;

#[rstest]
#[case("0.1", SchedrVersion::new(0, 1))]
#[case("1.12", SchedrVersion::new(1, 12))]
#[case("2.3.4", SchedrVersion::new(2, 3))]
#[case("2.3.0-rc1", SchedrVersion::new(2, 3))]
fn parses_major_minor_with_optional_patch(#[case] s: &str, #[case] expected: SchedrVersion) {
    assert_eq!(s.parse::<SchedrVersion>(), Ok(expected));
}

#[rstest]
#[case("")]
#[case("1")]
#[case("1.")]
#[case("a.b")]
#[case("1.2.")]
#[case("1.2.3.4")]
#[case("-1.2")]
fn rejects_malformed_versions(#[case] s: &str) {
    assert_eq!(s.parse::<SchedrVersion>(), Err(VersionError(s.to_owned())));
}

#[test]
fn current_is_the_crate_version() {
    let expected: SchedrVersion = env!("CARGO_PKG_VERSION").parse().unwrap();
    assert_eq!(SchedrVersion::current(), expected);
}

#[rstest]
#[case(SchedrVersion::new(1, 4), SchedrVersion::new(1, 4), true)]
#[case(SchedrVersion::new(1, 5), SchedrVersion::new(1, 4), true)]
#[case(SchedrVersion::new(1, 4), SchedrVersion::new(1, 5), false)]
#[case(SchedrVersion::new(2, 0), SchedrVersion::new(1, 9), false)]
#[case(SchedrVersion::new(1, 9), SchedrVersion::new(2, 0), false)]
fn can_read_same_major_and_equal_or_lower_minor(
    #[case] reader: SchedrVersion,
    #[case] produced: SchedrVersion,
    #[case] expected: bool,
) {
    assert_eq!(reader.can_read(produced), expected);
}

#[test]
fn displays_and_serializes_as_major_minor() {
    let v = SchedrVersion::new(1, 6);
    assert_eq!(v.to_string(), "1.6");
    assert_eq!(String::from(v), "1.6");
    assert_eq!(serde_json::to_value(v).unwrap(), serde_json::json!("1.6"));
    assert_eq!(
        serde_json::from_value::<SchedrVersion>(serde_json::json!("1.6")).unwrap(),
        v
    );
    assert_eq!(SchedrVersion::try_from("1.6.2".to_owned()), Ok(v));
}

#[test]
fn deserializing_a_malformed_version_fails() {
    assert!(serde_json::from_value::<SchedrVersion>(serde_json::json!("one")).is_err());
}

#[test]
fn error_message_names_the_input() {
    assert_eq!(
        VersionError("x".into()).to_string(),
        "`x` is not a schedr version (expected `major.minor` or `major.minor.patch`)"
    );
}

#[test]
fn schema_is_a_string_with_a_pattern() {
    let schema = serde_json::to_value(schema_for!(SchedrVersion)).unwrap();
    assert_eq!(schema["type"], "string");
    assert_eq!(schema["pattern"], r"^[0-9]+\.[0-9]+$");
}
