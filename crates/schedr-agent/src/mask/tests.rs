use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn set(values: &[(&str, &str)]) -> MaskSet {
    let set = MaskSet::new(5);
    for (label, value) in values {
        assert!(set.add(label, value), "{value} too short");
    }
    set
}

#[test]
fn values_are_masked_and_counted() {
    let set = set(&[("a", "hunter22"), ("b", "s3cr3t-token")]);
    let masked = set.mask_text("user hunter22 token s3cr3t-token again hunter22");
    assert_eq!(masked, "user **** token **** again ****");
    assert_eq!(set.counts(), [("a".to_owned(), 2), ("b".to_owned(), 1)]);
}

#[test]
fn a_short_value_isnt_masked() {
    let set = MaskSet::new(5);
    assert!(!set.add("flag", "true"));
    assert!(!set.add("empty", ""));
    assert!(set.is_empty());
    assert_eq!(set.mask_text("true"), "true");
}

#[test]
fn the_json_escaped_form_and_each_line_are_masked() {
    let set = set(&[("k", "pa\"ss\\word\nsecond-line\nx")]);
    // As JSON writes it (a JSON log line), and each long enough line on its own.
    assert_eq!(
        set.mask_text(r#"{"v": "pa\"ss\\word\nsecond-line\nx"}"#),
        r#"{"v": "****"}"#
    );
    assert_eq!(set.mask_text("> second-line <"), "> **** <");
    assert_eq!(set.mask_text("x"), "x");
}

#[test]
fn the_longest_value_wins() {
    let set = set(&[("short", "abcde"), ("long", "abcdefgh")]);
    assert_eq!(set.mask_text("abcdefgh abcde"), "**** ****");
    assert_eq!(
        set.counts(),
        [("short".to_owned(), 1), ("long".to_owned(), 1)]
    );
}

#[test]
fn a_value_split_across_chunks_is_masked() {
    let set = set(&[("a", "hunter22")]);
    let text = b"log: hunter22 and hunter22.";
    // Every split point, and byte by byte.
    for split in 0..=text.len() {
        let mut stream = set.stream();
        let mut out = stream.push(&text[..split]);
        out.extend(stream.push(&text[split..]));
        out.extend(stream.finish());
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "log: **** and ****.",
            "split {split}"
        );
    }
    let mut stream = set.stream();
    let mut out = Vec::new();
    for byte in text {
        out.extend(stream.push(&[*byte]));
    }
    out.extend(stream.finish());
    assert_eq!(String::from_utf8(out).unwrap(), "log: **** and ****.");
}

#[test]
fn overlapping_values_dont_leak_across_a_split() {
    // The end of one value begins another: a split there must not let either through.
    let set = set(&[("a", "abcde"), ("b", "dexyz")]);
    for split in 0..=7 {
        let mut stream = set.stream();
        let mut out = stream.push(&b"abcdexy"[..split]);
        out.extend(stream.push(&b"abcdexy"[split..]));
        out.extend(stream.finish());
        let out = String::from_utf8(out).unwrap();
        assert!(!out.contains("abcde"), "split {split}: {out}");
        assert_eq!(out, "****xy", "split {split}");
    }
}

#[test]
fn only_what_may_begin_a_value_is_held() {
    let set = set(&[("a", "hunter22")]);
    let mut stream = set.stream();
    assert_eq!(stream.push(b"progress 50%"), b"progress 50%");
    assert_eq!(stream.push(b" hun"), b" ");
    assert_eq!(stream.push(b"gry"), b"hungry");
    assert_eq!(stream.finish(), b"");
}

#[test]
fn json_strings_and_keys_are_masked() {
    let set = set(&[("a", "hunter22")]);
    let output = json!({"dsn": "u:hunter22@h", "hunter22": [1, "x hunter22"], "n": 5});
    assert_eq!(
        set.mask_json(&output),
        json!({"dsn": "u:****@h", "****": [1, "x ****"], "n": 5})
    );
    assert_eq!(set.counts(), [("a".to_owned(), 3)]);
}

#[test]
fn a_utf8_value_keeps_the_text_valid() {
    let set = set(&[("a", "zażółć")]);
    assert_eq!(set.mask_text("hasło: zażółć!"), "hasło: ****!");
}

#[test]
fn pythons_escaped_forms_are_masked() {
    // `json.dumps` (ASCII, `\uXXXX`, a surrogate pair beyond the BMP) and `repr` (an
    // exception's message), from the review of M4.6b.
    let set = set(&[
        ("u", "zażółć-gęślą"),
        ("q", "first'quote\"second"),
        ("e", "pass🔑word"),
    ]);
    let dumped = r#"{"credential": "zażółć-gęślą"}"#;
    assert_eq!(set.mask_text(dumped), r#"{"credential": "****"}"#);
    assert_eq!(
        set.mask_text(r#"KeyError('first\'quote"second')"#),
        "KeyError('****')"
    );
    assert_eq!(set.mask_text(r"x pass🔑word"), "x ****");
}

#[test]
fn a_repr_of_unprintable_characters_is_masked() {
    // CPython's `repr` escapes what `str.isprintable` refuses: `\xHH` up to U+00FF (a
    // no-break space, a soft hyphen), `\uXXXX`, `\UXXXXXXXX`; it keeps the printable ones
    // (from Grok's security review of M4.6b; the forms below are CPython's).
    let cases = [
        ("pass\u{a0}word", r"pass\xa0word"),
        ("soft\u{ad}hy", r"soft\xadhy"),
        ("zw\u{200b}sp", r"zw\u200bsp"),
        ("pua\u{e000}x", r"pua\ue000x"),
        ("line\u{2028}x", r"line\u2028x"),
        ("nonbmp\u{e0001}x", r"nonbmp\U000e0001x"),
        ("emoji\u{1f600}x", "emoji\u{1f600}x"),
        ("tab\tq\u{7f}", r"tab\tq\x7f"),
    ];
    for (value, repr) in cases {
        let set = set(&[("v", value)]);
        assert_eq!(
            set.mask_text(&format!("KeyError('{repr}')")),
            "KeyError('****')",
            "{value:?}"
        );
    }
}

#[test]
fn a_number_equal_to_a_value_is_masked() {
    let set = set(&[("n", "83912746")]);
    assert_eq!(
        set.mask_json(&json!({"pin": 83_912_746, "n": 5})),
        json!({"pin": "****", "n": 5})
    );
    assert_eq!(set.mask_json(&json!(1.5)), json!(1.5));
}

#[test]
fn keys_masked_alike_stay_apart() {
    let set = set(&[("a", "secret-a"), ("b", "secret-b")]);
    let output = json!({"secret-a": 10, "secret-b": 20, "**** (2)": 1, "ordinary": 30});
    let masked = set.mask_json(&output);
    let masked = masked.as_object().unwrap();
    assert_eq!(masked.len(), 4, "{masked:?}");
    assert_eq!(masked["ordinary"], 30);
    assert_eq!(masked["**** (2)"], 1);
    let mut values: Vec<_> = masked.values().filter_map(Value::as_i64).collect();
    values.sort_unstable();
    assert_eq!(values, [1, 10, 20, 30]);
}

#[test]
fn many_values_mask_a_large_stream_quickly() {
    // Indexed by first byte: bytes no value starts with pass at once.
    let set = MaskSet::new(5);
    for i in 0..1000 {
        assert!(set.add(&format!("s{i}"), &format!("Zq{i:06}-secret")));
    }
    let text = "log line without any secret in it, 0123456789\n".repeat(20_000);
    let started = std::time::Instant::now();
    let mut stream = set.stream();
    let mut out = Vec::new();
    for chunk in text.as_bytes().chunks(64 * 1024) {
        out.extend(stream.push(chunk));
    }
    out.extend(stream.finish());
    assert_eq!(out, text.as_bytes());
    // Generous for a debug build; the unindexed scan took seconds here.
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
}
