use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

#[test]
fn a_structured_line_reads_back_and_any_other_is_plain() {
    let mut line = LogLine::new(Level::Warning, "pod pending");
    line.fields.insert("node".to_owned(), json!("n1"));
    line.time = Some("2026-11-21T10:00:03.112Z".to_owned());
    let written = line.json();
    assert!(written.starts_with(r#"{"schedr_log":1,"#), "{written}");
    assert_eq!(LogLine::parse(&format!("{written}\n")), Some(line));
    for plain in [
        "hello",
        r#"{"other": 1}"#,
        r#"{"schedr_log": 2, "level": "info", "message": "x"}"#,
        r#"{"schedr_log": 1, "level": "loud", "message": "x"}"#,
        "{\"schedr_log\": 1, broken",
    ] {
        assert_eq!(LogLine::parse(plain), None, "{plain}");
    }
}

#[test]
fn its_readable_form_quotes_what_needs_it_and_indents_a_traceback() {
    let line: LogLine = serde_json::from_value(json!({
        "schedr_log": 1, "level": "error", "message": "failed", "time": "2026-11-21T10:00:00Z",
        "fields": {"logger": "etl", "phase": "Pending", "note": "two words", "tries": 3,
                   "traceback": "Traceback:\n  File x\nValueError: no\n"}
    }))
    .unwrap();
    assert_eq!(
        line.plain(),
        "2026-11-21T10:00:00Z ERROR    failed  logger=etl  note=\"two words\"  phase=Pending  \
         tries=3\n  traceback:\n    Traceback:\n      File x\n    ValueError: no"
    );
}

#[test]
fn a_log_is_rendered_line_by_line() {
    let structured = LogLine::new(Level::Info, "started").json();
    let text = format!("plain one\n{structured}\nlast, no newline");
    assert_eq!(
        rendered(&text),
        "plain one\nINFO     started\nlast, no newline"
    );
    assert_eq!(Level::parse("WARN"), Some(Level::Warning));
}
