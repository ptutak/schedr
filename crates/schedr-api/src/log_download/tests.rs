use pretty_assertions::assert_eq;

use super::*;

#[test]
fn a_line_longer_than_a_part_is_passed_on_whole_when_it_ends() {
    // Codex's review of M5.2i5: a 2 MiB structured line was a truncation note.
    let mut lines = Lines::default();
    let long = "x".repeat(3 << 20);
    let part = |range: std::ops::Range<usize>| long.as_bytes()[range].to_vec();
    let mut out = lines.take(b"first\n", false);
    out += &lines.take(&part(0..(1 << 20)), false);
    assert_eq!(out, "first\n", "held until it ends");
    out += &lines.take(&part((1 << 20)..long.len()), false);
    out += &lines.take(b"\nlast", true);
    assert_eq!(out, format!("first\n{long}\nlast"));
}

#[test]
fn a_secret_is_masked_whole_however_the_parts_fall() {
    let mut lines = Lines::default();
    let mut out = lines.take(b"connecting to postgres://u:hun", false);
    out += &lines.take(b"ter2@db/x\nok\n", true);
    assert!(!out.contains("hunter2"), "{out}");
    assert!(out.ends_with("ok\n"), "{out}");
}

#[test]
fn the_masking_context_runs_on_across_a_part_passing_on_a_short_line() {
    // agy's review of M5.2i5: a part ending one short line forgot the quoted key before it.
    let mut lines = Lines::default();
    let mut out = lines.take(b"private_key=\"line1\n", false);
    let mut second = b"line2\n".to_vec();
    second.extend(std::iter::repeat_n(b'b', 4096));
    out += &lines.take(&second, false);
    out += &lines.take(b"SUPER_SECRET_KEY_MATERIAL\"\nafter\n", true);
    assert!(
        !out.contains("SUPER_SECRET"),
        "{}",
        &out[out.len().saturating_sub(80)..]
    );
    assert!(
        out.ends_with("after\n"),
        "{}",
        &out[out.len().saturating_sub(80)..]
    );
}

#[test]
fn a_line_past_the_limit_is_cut_between_tokens_never_inside_one() {
    // Codex's second review of M5.2i5: cut at the limit, a password's `@` came after it.
    let mut lines = Lines::default();
    let filler = "x ".repeat(LINE_MAX / 2);
    let mut out = lines.take(filler.as_bytes(), false);
    out += &lines.take(b"postgres://u:hunter2", false);
    out += &lines.take(b"@db/x\n", true);
    assert!(!out.contains("hunter2"), "{}", &out[out.len() - 80..]);
    assert!(
        out.ends_with("@db/x\n") || out.contains("postgres://u:"),
        "the rest kept"
    );
}

#[test]
fn a_token_past_the_limit_is_not_shown() {
    let mut lines = Lines::default();
    let token = "y".repeat(LINE_MAX + 10);
    let mut out = lines.take(token.as_bytes(), false);
    out += &lines.take(b"tail\nnext\n", true);
    assert!(!out.contains("yyyy"), "nothing of it");
    assert!(out.contains("not shown"), "{out}");
    assert!(out.ends_with("next\n"), "{out}");
}
