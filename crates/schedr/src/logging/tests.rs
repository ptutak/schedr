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

/// B203's review: sqlx's warning of an unknown URL parameter quotes its value (a password):
/// the filter keeps it out, whatever the level.
#[test]
fn sqlx_connect_option_warnings_are_never_logged() {
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt;
    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let buffer = Buffer::default();
    let writer = buffer.clone();
    // A level naming the target itself, which a directive added to it wouldn't outweigh.
    let level = "debug,sqlx_postgres::options::parse=trace";
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(level_filter(level).unwrap())
        .with_writer(move || writer.clone())
        .finish()
        .with(quiet_sqlx_options());
    tracing::subscriber::with_default(subscriber, || {
        tracing::warn!(target: "sqlx_postgres::options::parse", value = "pw-s3cret", "ignoring");
        tracing::warn!(target: "schedr_bin", "shown");
    });
    let logged = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert!(!logged.contains("pw-s3cret"), "{logged}");
    assert!(logged.contains("shown"), "{logged}");
}
