use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

#[rstest]
#[case(LogFormat::Auto, true, ResolvedFormat::Text)]
#[case(LogFormat::Auto, false, ResolvedFormat::Json)]
#[case(LogFormat::Json, true, ResolvedFormat::Json)]
#[case(LogFormat::Text, false, ResolvedFormat::Text)]
fn auto_is_text_in_a_terminal_and_json_otherwise(
    #[case] format: LogFormat,
    #[case] terminal: bool,
    #[case] expected: ResolvedFormat,
) {
    assert_eq!(resolve_format(format, terminal), expected);
}

#[rstest]
#[case("info")]
#[case("warn,schedr_master=debug")]
#[case("debug")]
fn accepts_rust_log_style_levels(#[case] level: &str) {
    assert!(level_filter(level).is_ok());
}

#[test]
fn rejects_invalid_levels_naming_the_key() {
    let err = level_filter("schedr=loud").unwrap_err();
    assert!(err.starts_with("[log] level `schedr=loud`: "), "{err}");
}

// Each test runs in its own process (cargo-nextest), so each can install the global logger.
#[test]
fn init_installs_the_json_logger_once() {
    let section = LogSection {
        format: LogFormat::Json,
        level: "info".into(),
    };
    assert_eq!(init(&section, false), Ok(true));
    assert_eq!(init(&section, false), Ok(false));
}

#[test]
fn init_installs_the_text_logger() {
    let section = LogSection {
        format: LogFormat::Text,
        level: "debug".into(),
    };
    assert_eq!(init(&section, true), Ok(true));
}

#[test]
fn init_rejects_an_invalid_level() {
    let section = LogSection {
        format: LogFormat::Auto,
        level: "x=y=z".into(),
    };
    assert!(init(&section, true).is_err());
}
