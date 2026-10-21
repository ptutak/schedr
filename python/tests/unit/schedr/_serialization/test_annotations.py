from collections.abc import Sequence
from typing import Annotated, Any, NewType, Optional, Union

import pytest

from schedr._serialization.annotations import TypeModelError, is_union, label, type_hints, unwrap

type Alias = list[int]
type Outer = Alias
type Generic[T] = list[T]
UserId = NewType("UserId", int)
OPTIONAL_INT = Optional[int]  # noqa: UP045 - testing the old spelling
UNION_INT_STR = Union[int, str]  # noqa: UP007 - testing the old spelling


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (Alias, list[int]),
        (Outer, list[int]),
        (Annotated[int, "meta"], int),
        (UserId, int),
        (type(None), None),
        (None, None),
        (str, str),
    ],
)
def test_unwrap_resolves_renaming_wrappers(annotation: object, expected: object) -> None:
    assert unwrap(annotation) == expected


def test_unwrap_rejects_generic_aliases() -> None:
    with pytest.raises(TypeModelError, match=r"generic type alias .* isn't supported yet"):
        unwrap(Generic[int])


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (int | None, True),
        (OPTIONAL_INT, True),
        (UNION_INT_STR, True),
        (int, False),
        (list[int], False),
    ],
)
def test_is_union(annotation: object, expected: bool) -> None:
    assert is_union(annotation) is expected


@pytest.mark.parametrize(
    ("annotation", "text"),
    [
        (None, "None"),
        (type(None), "None"),
        (Any, "Any"),
        (int, "int"),
        (dict[int, str], "dict[int, str]"),
        (Sequence[int], "Sequence[int]"),
        (OPTIONAL_INT, "int | None"),
    ],
)
def test_label(annotation: object, text: str) -> None:
    assert label(annotation) == text


def test_type_hints_resolves_and_reports_bad_annotations() -> None:
    def good(a: int) -> str: ...

    def bad(a: Missing) -> None: ...  # type: ignore[name-defined]  # noqa: F821

    assert type_hints(good, "x") == {"a": int, "return": str}
    with pytest.raises(TypeModelError, match=r"task `bad`: can't resolve an annotation: .*Missing"):
        type_hints(bad, "task `bad`")
