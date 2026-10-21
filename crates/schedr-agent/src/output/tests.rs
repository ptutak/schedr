use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

#[test]
fn keeps_everything_under_the_limit() {
    let mut buffer = TailBuffer::new(10);
    buffer.push(b"hello ");
    buffer.push(b"you");
    assert_eq!(buffer.text(), "hello you");
    assert!(!buffer.truncated());
}

#[test]
fn keeps_the_last_part_over_the_limit() {
    let mut buffer = TailBuffer::new(5);
    buffer.push(b"abc");
    buffer.push(b"defgh");
    assert_eq!(buffer.text(), "defgh");
    assert!(buffer.truncated());
}

#[test]
fn exactly_at_the_limit_is_not_truncated() {
    let mut buffer = TailBuffer::new(3);
    buffer.push(b"abc");
    assert!(!buffer.truncated());
}

#[test]
fn a_character_cut_at_the_start_is_dropped() {
    let mut buffer = TailBuffer::new(5);
    buffer.push("ażółć".as_bytes()); // 9 bytes: a ż ó ł ć, two bytes each but `a`
    assert_eq!(buffer.text(), "łć");
    assert!(buffer.truncated());
}

#[test]
fn invalid_utf8_is_replaced() {
    let mut buffer = TailBuffer::new(10);
    buffer.push(&[b'o', b'k', 0xff]);
    assert_eq!(buffer.text(), "ok\u{fffd}");
}

#[test]
fn an_empty_buffer_is_empty() {
    assert_eq!(TailBuffer::new(4).text(), "");
}

#[test]
fn signals_map_to_shell_style_exit_codes() {
    assert_eq!(signal_exit_code(9), 137);
    assert_eq!(signal_exit_code(15), 143);
}

#[test]
fn process_result_has_the_documented_fields() {
    let mut out = TailBuffer::new(2);
    out.push(b"abc");
    let err = TailBuffer::new(10);
    assert_eq!(
        process_result(&out, &err, 3),
        json!({
            "stdout": "bc", "stderr": "", "exit_code": 3,
            "stdout_truncated": true, "stderr_truncated": false,
        })
    );
}
