import json
import shlex
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from schedr._model import DagCtxField, DagCtxRef, OutputRef, Template, TemplateRef
from schedr._templates import RAW, as_text, check_spec, is_tstring, to_template
from schedr.context import dag_ctx
from schedr.tasks import Output

PLANNED_FOR = DagCtxRef(DagCtxField.PLANNED_FOR)


def test_is_tstring() -> None:
    assert is_tstring(t"x")
    assert not is_tstring("x")
    assert not is_tstring(None)


def test_text_and_references_become_parts() -> None:
    out = Output[int]("count")
    template = to_template(t"./notify.sh {dag_ctx.run.planned_for} {out}", "w", shell=False)
    assert template == Template(
        ["./notify.sh ", TemplateRef(PLANNED_FOR), " ", TemplateRef(OutputRef("count"))]
    )


def test_format_specs_are_kept_on_references() -> None:
    template = to_template(t"day={dag_ctx.run.planned_for:%Y-%m-%d}", "w", shell=False)
    assert template == Template(["day=", TemplateRef(PLANNED_FOR, "%Y-%m-%d")])
    shell = to_template(t"./run.sh {Output[str]('flags'):raw}", "w", shell=True)
    assert shell.parts[1] == TemplateRef(OutputRef("flags"), RAW)


def test_literal_values_are_formatted_at_parse_time() -> None:
    region, n, when = "eu west", 7, date(2026, 1, 2)
    template = to_template(t"--region {region} -n {n:03d} {when:%d.%m} {n!r}", "w", shell=False)
    assert template == Template(["--region eu west -n 007 02.01 7"])


def test_literals_in_shell_commands_are_quoted_unless_raw() -> None:
    path, flags = "a b;rm -rf ~", "-v -x"
    template = to_template(t"./run.sh {path} {flags:raw}", "w", shell=True)
    assert template == Template(["./run.sh 'a b;rm -rf ~' -v -x"])


def test_conversions_on_literals() -> None:
    name = "x"
    assert to_template(t"{name!s}{name!a}", "w", shell=False) == Template(["x'x'"])


@pytest.mark.parametrize(
    ("value", "text"),
    [
        ("a b", "a b"),
        (7, "7"),
        (1.5, "1.5"),
        (1.0, "1.0"),
        (1e20, "1e+20"),
        (True, "True"),
        (datetime(2026, 10, 19, 3, tzinfo=UTC), "2026-10-19T03:00:00+00:00"),
        (date(2026, 10, 19), "2026-10-19"),
        (timedelta(hours=1), "3600"),
        (timedelta(milliseconds=1500), "1.5"),
        (timedelta(days=-1), "-86400"),
    ],
)
def test_literals_are_written_like_the_agent_writes_references(value: object, text: str) -> None:
    assert as_text(value) == text
    assert to_template(t"{value}", "w", shell=False) == Template([text])


def test_empty_text_pieces_are_dropped() -> None:
    out = Output[int]("a")
    assert to_template(t"{out}", "w", shell=False) == Template([TemplateRef(OutputRef("a"))])


@pytest.mark.parametrize(
    ("make", "error", "message"),
    [
        (lambda: t"{None}", TypeError, r"`\{None\}` is NoneType; t-strings take references"),
        (lambda: t"{[1, 2]}", TypeError, "is list"),
        (lambda: t"{t'inner'}", TypeError, "nesting isn't supported"),
        (
            lambda: t"{dag_ctx.run.planned_for!r}",
            TypeError,
            "conversions don't apply to references",
        ),
        (lambda: t"{dag_ctx.run.planned_for:>10}", ValueError, r"unknown format spec `:>10`"),
        (lambda: t"{dag_ctx.run.planned_for:raw}", ValueError, r"`:raw` only applies to shell"),
        (lambda: t"{'x':raw}", ValueError, r"`:raw` only applies to shell commands"),
    ],
)
def test_invalid_t_strings_are_parse_errors(
    make: object, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        to_template(make(), "w", shell=False)  # type: ignore[operator]


@pytest.mark.parametrize("spec", ["", "%Y-%m-%d", "%H:%M"])
def test_check_spec_accepts_none_and_strftime(spec: str) -> None:
    check_spec(spec, shell=False, where="w")


def test_check_spec_raw_only_in_shell() -> None:
    check_spec("raw", shell=True, where="w")
    with pytest.raises(ValueError, match="w: `:raw` only applies to shell commands"):
        check_spec("raw", shell=False, where="w")


def test_literals_that_format_to_nothing_add_no_text() -> None:
    empty = ""
    assert to_template(t"{empty}x", "w", shell=False) == Template(["x"])


def test_the_shared_shell_quoting_fixture_matches_shlex_quote() -> None:
    """``testdata/shell_quote.json`` is also checked by the Rust tests (render.rs)."""
    fixture = Path(__file__).parents[4] / "testdata" / "shell_quote.json"
    cases = json.loads(fixture.read_text())["cases"]
    assert len(cases) > 20
    for case in cases:
        assert shlex.quote(case["input"]) == case["quoted"], case
