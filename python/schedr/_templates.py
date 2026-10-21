"""t-strings (PEP 750) as DAG JSON templates: literal text plus references.

References (outputs, ``dag_ctx``) are filled in at run time by the agent or the runner;
plain Python values known in the DAG body are formatted at parse time. In a shell command
every interpolated value is shell-quoted unless its format spec is ``raw`` (see PROJECT.md,
Airflow compatibility, Scope: Templating, safe substitution into shell commands).
"""

import shlex
from datetime import date, datetime, timedelta
from string.templatelib import Interpolation
from string.templatelib import Template as TString

from schedr._model import Reference, Template, TemplatePart, TemplateRef

RAW = "raw"
"""The format spec that inserts a value into a shell command unquoted."""

_LITERAL_TYPES = (str, int, float, bool, datetime, date, timedelta)


def is_tstring(value: object) -> bool:
    """Whether ``value`` is a t-string (``t"..."``).

    Args:
        value: Any object.

    Returns:
        True for ``string.templatelib.Template`` instances.
    """
    return isinstance(value, TString)


def check_spec(spec: str, *, shell: bool, where: str) -> None:
    """Checks a reference's format spec.

    Allowed: none; ``raw`` in a shell command; a ``strftime`` pattern (containing ``%``),
    whose fit with the reference's type is checked once the type is known.

    Args:
        spec: The format spec, possibly empty.
        shell: Whether the template is a shell command.
        where: Where it is used, for messages.

    Raises:
        ValueError: For ``raw`` outside a shell command, or an unknown spec.
    """
    if not spec or "%" in spec:
        return
    if spec == RAW:
        if not shell:
            raise ValueError(
                f"{where}: `:raw` only applies to shell commands (elsewhere nothing is quoted)"
            )
        return
    raise ValueError(
        f"{where}: unknown format spec `:{spec}` for a reference (use `:raw` in shell commands, "
        "or a strftime pattern such as `:%Y-%m-%d` for dates)"
    )


def as_text(value: object) -> str:
    """A value written as text the way the agent writes references (``render.rs``).

    Strings as they are; numbers and booleans as ``str()`` does; ``datetime`` and ``date`` in
    ISO 8601 (``2026-10-19T03:00:00+00:00``); ``timedelta`` as total seconds (``3600``,
    ``1.5``).

    Args:
        value: The value.

    Returns:
        Its text.
    """
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, timedelta):
        micros = (value.days * 86_400 + value.seconds) * 1_000_000 + value.microseconds
        seconds, rest = divmod(micros, 1_000_000)
        return str(seconds) if rest == 0 else str(micros / 1_000_000)
    return str(value)


def _format_literal(item: Interpolation[object], *, shell: bool, where: str) -> str:
    value = item.value
    if value is None or not isinstance(value, _LITERAL_TYPES):
        raise TypeError(
            f"{where}: `{{{item.expression}}}` is {type(value).__name__}; t-strings take "
            "references, or strings, numbers, booleans, dates and durations"
        )
    if item.conversion == "r":
        value = repr(value)
    elif item.conversion == "s":
        value = str(value)
    elif item.conversion == "a":
        value = ascii(value)
    spec = item.format_spec
    if spec == RAW:
        if not shell:
            raise ValueError(f"{where}: `:raw` only applies to shell commands")
        return as_text(value)
    text = as_text(value) if not spec else format(value, spec)
    return shlex.quote(text) if shell else text


def to_template(tstring: TString, where: str, *, shell: bool) -> Template:
    """Converts a t-string to a DAG JSON template.

    Args:
        tstring: The t-string.
        where: Where it is used, for messages.
        shell: Whether it is a shell command (literal values are then shell-quoted, and
            ``:raw`` is allowed).

    Returns:
        The template, adjacent text merged into one part.

    Raises:
        TypeError: For an interpolated value that is neither a reference nor a simple value,
            a nested t-string, or a conversion (``!r``) on a reference.
        ValueError: For an unknown format spec.
    """
    parts: list[TemplatePart] = []

    def add_text(text: str) -> None:
        if not text:
            return
        if parts and isinstance(parts[-1], str):
            parts[-1] += text
        else:
            parts.append(text)

    for item in tstring:
        if isinstance(item, str):
            add_text(item)
            continue
        value = item.value
        if callable(getattr(value, "_schedr_reference", None)):
            if item.conversion is not None:
                raise TypeError(
                    f"{where}: `{{{item.expression}!{item.conversion}}}`: conversions don't apply "
                    "to references (they are rendered at run time, without Python)"
                )
            check_spec(item.format_spec, shell=shell, where=where)
            reference: Reference = value._schedr_reference()
            parts.append(TemplateRef(reference, item.format_spec or None))
        elif is_tstring(value):
            raise TypeError(
                f"{where}: `{{{item.expression}}}` is a t-string; nesting isn't supported"
            )
        else:
            add_text(_format_literal(item, shell=shell, where=where))
    return Template(parts)
