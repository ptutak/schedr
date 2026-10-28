use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

#[rstest]
#[case::plain("hello", "hello")]
#[case::escaped("<b>&\"'", "&lt;b&gt;&amp;&quot;&#39;")]
#[case::colour("\u{1b}[31mred\u{1b}[0m ok", "<span class=\"fg-1\">red</span> ok")]
#[case::bright_and_bold("\u{1b}[1;92mgo", "<span class=\"fg-10 bold\">go</span>")]
#[case::changed_midway(
    "\u{1b}[32ma\u{1b}[1mb\u{1b}[22;39mc",
    "<span class=\"fg-2\">a</span><span class=\"fg-2 bold\">b</span>c"
)]
#[case::background(
    "\u{1b}[44;37mx\u{1b}[49mz\u{1b}[m",
    "<span class=\"fg-7 bg-4\">x</span><span class=\"fg-7\">z</span>"
)]
#[case::rgb_and_256_skipped(
    "\u{1b}[38;2;1;2;3;4mu\u{1b}[48;5;200;3mi",
    "<span class=\"underline\">u</span><span class=\"italic underline\">i</span>"
)]
#[case::other_sequences_removed(
    "a\u{1b}[2Kb\u{1b}]0;title\u{7}c\u{1b}]8;;x\u{1b}\\d\u{1b}(Be",
    "abcde"
)]
#[case::unterminated("a\u{1b}[31", "a")]
#[case::trailing_escape("a\u{1b}", "a")]
#[case::reset_with_nothing_open("\u{1b}[0mtext", "text")]
fn ansi_becomes_spans(#[case] text: &str, #[case] html: &str) {
    assert_eq!(to_html(text), html);
}

#[test]
fn a_tails_parts_carry_the_colour_and_a_cut_escape() {
    let mut converter = Converter::new();
    assert_eq!(converter.push("plain \u{1b}"), "plain ");
    assert_eq!(converter.push("[31mred"), "<span class=\"fg-1\">red</span>");
    // Still red in the next part, until reset.
    assert_eq!(
        converter.push("more\u{1b}[0m done \u{1b}[1"),
        "<span class=\"fg-1\">more</span> done "
    );
    assert_eq!(
        converter.push(";32mbold"),
        "<span class=\"fg-2 bold\">bold</span>"
    );
    // An OSC cut in two, and a two-byte escape.
    assert_eq!(
        converter.push("\u{1b}]0;ti"),
        "<span class=\"fg-2 bold\"></span>"
    );
    assert_eq!(
        converter.push("tle\u{7}x\u{1b}(Bz"),
        "<span class=\"fg-2 bold\">xz</span>"
    );
}
