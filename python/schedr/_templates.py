"""t-strings (PEP 750) as DAG JSON templates: literal text plus references.

References (outputs, ``dag_ctx``) are filled in at run time by the agent or the runner;
plain Python values known in the DAG body are formatted at parse time. A shell command takes
no placeholders: values are passed as arguments or in the environment, never as shell source
(see PROJECT.md, Airflow compatibility, Scope: Templating, decision 99).
"""

from datetime import date, datetime, timedelta
from string.templatelib import Interpolation
from string.templatelib import Template as TString

from schedr._model import Reference, Template, TemplatePart, TemplateRef

_LITERAL_TYPES = (str, int, float, bool, datetime, date, timedelta)


def is_tstring(value: object) -> bool:
    """Whether ``value`` is a t-string (``t"..."``).

    Args:
        value: Any object.

    Returns:
        True for ``string.templatelib.Template`` instances.
    """
    return isinstance(value, TString)


def check_spec(spec: str, *, where: str) -> None:
    """Checks a reference's format spec.

    Allowed: none, or a ``strftime`` pattern (containing ``%``), whose fit with the
    reference's type is checked once the type is known.

    Args:
        spec: The format spec, possibly empty.
        where: Where it is used, for messages.

    Raises:
        ValueError: For an unknown spec.
    """
    if not spec or "%" in spec:
        return
    raise ValueError(
        f"{where}: unknown format spec `:{spec}` for a reference (a strftime pattern such as "
        "`:%Y-%m-%d` formats dates)"
    )


def shell_text(tstring: TString, where: str) -> str:
    """The text of a t-string used as a shell command, which takes no placeholders.

    A value placed in shell source can run as code whatever the quoting (inside the author's
    quotes, in a comment, a heredoc, after a backslash), so a shell command takes values only
    as arguments or environment variables (decision 99).

    Args:
        tstring: The t-string.
        where: Where it is used, for messages.

    Returns:
        Its text.

    Raises:
        TypeError: For a placeholder, with the two ways to pass the value instead.
    """
    for item in tstring:
        if isinstance(item, Interpolation):
            conversion = f"!{item.conversion}" if item.conversion else ""
            spec = f":{item.format_spec}" if item.format_spec else ""
            raise TypeError(
                f"{where}: `{{{item.expression}{conversion}{spec}}}` in a shell command: the "
                "shell would read the value as code. Pass values as arguments, "
                'command=["./notify.sh", out.report_url] (no shell runs), or in the '
                """environment, command='./notify.sh "$URL"', env={"URL": out.report_url}"""
            )
    return "".join(tstring.strings)


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


def _format_literal(item: Interpolation[object], where: str) -> str:
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
    return as_text(value) if not spec else format(value, spec)


def to_template(tstring: TString, where: str) -> Template:
    """Converts a t-string to a DAG JSON template.

    Args:
        tstring: The t-string.
        where: Where it is used, for messages.

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
            check_spec(item.format_spec, where=where)
            reference: Reference = value._schedr_reference()
            parts.append(TemplateRef(reference, item.format_spec or None))
        elif is_tstring(value):
            raise TypeError(
                f"{where}: `{{{item.expression}}}` is a t-string; nesting isn't supported"
            )
        else:
            add_text(_format_literal(item, where))
    return Template(parts)
