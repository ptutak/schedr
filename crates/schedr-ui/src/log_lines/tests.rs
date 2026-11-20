use pretty_assertions::assert_eq;

use super::*;

#[test]
fn structured_lines_show_their_parts_escaped_and_plain_ones_stay() {
    let line = r#"{"schedr_log":1,"level":"warning","message":"a <b>","fields":{"node":"n&1"}}"#;
    let text = format!("before\n{line}\nafter\n");
    let html = to_html(&mut ansi::Converter::default(), &text);
    assert_eq!(
        html,
        "before\n<span class=\"sl lv-warning\"><span class=\"sl-level\">WARNING</span> \
         <span class=\"sl-message\">a &lt;b&gt;</span> <span class=\"sl-field\">\
         <span class=\"sl-name\">node</span>=n&amp;1</span>\n</span>after\n"
    );
}

#[test]
fn a_traceback_field_goes_below_its_line() {
    let line =
        r#"{"schedr_log":1,"level":"error","message":"failed","fields":{"traceback":"T\n  x\n"}}"#;
    let html = to_html(&mut ansi::Converter::default(), line);
    assert!(
        html.ends_with("<span class=\"sl-long\"><span class=\"sl-name\">traceback</span>:\nT\n  x</span></span>"),
        "{html}"
    );
}

#[test]
fn a_download_keeps_a_character_split_between_chunks_whole() {
    // Codex's second review of M5.2i5: "é" split in two came out as two U+FFFD.
    let e = "é\n".as_bytes();
    let mut json = Download::new(false);
    let out = json.take(&e[..1], false) + &json.take(&e[1..], false) + &json.take(b"", true);
    assert_eq!(out, "é\n");
}

#[test]
fn a_plain_download_renders_whole_lines_and_passes_a_long_one_on() {
    let line = r#"{"schedr_log":1,"level":"info","message":"hi"}"#;
    let mut plain = Download::new(true);
    let (first, second) = line.split_at(10);
    let mut out = plain.take(first.as_bytes(), false);
    assert_eq!(out, "", "held until it ends");
    out += &plain.take(format!("{second}\n").as_bytes(), false);
    assert_eq!(out, "INFO     hi\n");
    // Codex's second review: a line with no end was held whole.
    let long = vec![b'z'; DOWNLOAD_LINE_MAX + 1];
    let passed = plain.take(&long, false);
    assert_eq!(passed.len(), long.len(), "passed on, not held");
    let after = plain.take(format!("zz\n{line}\n").as_bytes(), false);
    assert_eq!(after, "zz\nINFO     hi\n", "rendered again after its end");
    // agy's review of M5.2i5: a structured line split across parts right after the long
    // line's end came out raw.
    let _ = plain.take(&long, false);
    let (head, tail) = line.split_at(20);
    let mut split = plain.take(format!("end\n{head}").as_bytes(), false);
    split += &plain.take(format!("{tail}\n").as_bytes(), false);
    split += &plain.take(b"", true);
    assert_eq!(split, "end\nINFO     hi\n");
    // The same when the log ends within the long line's part.
    let _ = plain.take(&long, false);
    assert_eq!(
        plain.take(format!("end\n{line}\n").as_bytes(), true),
        "end\nINFO     hi\n"
    );
}
