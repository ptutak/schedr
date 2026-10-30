from datetime import UTC, date, datetime, timedelta

import pytest

from schedr._model import DagCtxField, DagCtxRef, OutputRef, Template, TemplateRef
from schedr._templates import as_text, check_spec, is_tstring, shell_text, to_template
from schedr.context import DagCtxReference
from schedr.tasks import Output

PLANNED_FOR_REF = DagCtxReference(DagCtxField.PLANNED_FOR)
"""What ``dag_ctx.run.planned_for`` gives in a DAG body."""

PLANNED_FOR = DagCtxRef(DagCtxField.PLANNED_FOR)


def test_is_tstring() -> None:
    assert is_tstring(t"x")
    assert not is_tstring("x")
    assert not is_tstring(None)


def test_text_and_references_become_parts() -> None:
    out = Output[int]("count")
    template = to_template(t"./notify.sh {PLANNED_FOR_REF} {out}", "w")
    assert template == Template(
        ["./notify.sh ", TemplateRef(PLANNED_FOR), " ", TemplateRef(OutputRef("count"))]
    )


def test_format_specs_are_kept_on_references() -> None:
    template = to_template(t"day={PLANNED_FOR_REF:%Y-%m-%d}", "w")
    assert template == Template(["day=", TemplateRef(PLANNED_FOR, "%Y-%m-%d")])


def test_literal_values_are_formatted_at_parse_time() -> None:
    region, n, when = "eu west", 7, date(2026, 1, 2)
    template = to_template(t"--region {region} -n {n:03d} {when:%d.%m} {n!r}", "w")
    assert template == Template(["--region eu west -n 007 02.01 7"])


def test_a_shell_command_is_text_without_placeholders() -> None:
    assert shell_text(t"echo '{{}}' $HOME", "w") == "echo '{}' $HOME"


@pytest.mark.parametrize(
    ("make", "placeholder"),
    [
        (lambda: t"echo '{Output[str]('url')}'", "{Output[str]('url')}"),
        (lambda: t"echo {'x'!r}", "{'x'!r}"),
        (lambda: t"# {PLANNED_FOR_REF:%Y}", "{PLANNED_FOR_REF:%Y}"),
        (lambda: t"{''}", "{''}"),
    ],
)
def test_a_shell_command_refuses_every_placeholder(make: object, placeholder: str) -> None:
    with pytest.raises(TypeError) as raised:
        shell_text(make(), "w")  # type: ignore[operator]
    assert str(raised.value).startswith(f"w: `{placeholder}` in a shell command:")


def test_conversions_on_literals() -> None:
    name = "x"
    assert to_template(t"{name!s}{name!a}", "w") == Template(["x'x'"])


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
    assert to_template(t"{value}", "w") == Template([text])


def test_empty_text_pieces_are_dropped() -> None:
    out = Output[int]("a")
    assert to_template(t"{out}", "w") == Template([TemplateRef(OutputRef("a"))])


@pytest.mark.parametrize(
    ("make", "error", "message"),
    [
        (lambda: t"{None}", TypeError, r"`\{None\}` is NoneType; t-strings take references"),
        (lambda: t"{[1, 2]}", TypeError, "is list"),
        (lambda: t"{t'inner'}", TypeError, "nesting isn't supported"),
        (
            lambda: t"{PLANNED_FOR_REF!r}",
            TypeError,
            "conversions don't apply to references",
        ),
        (lambda: t"{PLANNED_FOR_REF:>10}", ValueError, r"unknown format spec `:>10`"),
        (lambda: t"{PLANNED_FOR_REF:raw}", ValueError, r"unknown format spec `:raw`"),
        (lambda: t"{'x':raw}", ValueError, r"Invalid format specifier 'raw'"),
    ],
)
def test_invalid_t_strings_are_parse_errors(
    make: object, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        to_template(make(), "w")  # type: ignore[operator]


@pytest.mark.parametrize("spec", ["", "%Y-%m-%d", "%H:%M"])
def test_check_spec_accepts_none_and_strftime(spec: str) -> None:
    check_spec(spec, where="w")


def test_literals_that_format_to_nothing_add_no_text() -> None:
    empty = ""
    assert to_template(t"{empty}x", "w") == Template(["x"])
