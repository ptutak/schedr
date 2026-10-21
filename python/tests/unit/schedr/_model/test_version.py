from importlib.metadata import version

import pytest

from schedr._model.version import SchedrVersion


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0.1", SchedrVersion(0, 1)),
        ("1.12", SchedrVersion(1, 12)),
        ("2.3.4", SchedrVersion(2, 3)),
        ("2.3.0-rc1", SchedrVersion(2, 3)),
    ],
)
def test_parse_accepts_major_minor_with_optional_patch(text: str, expected: SchedrVersion) -> None:
    assert SchedrVersion.parse(text) == expected


@pytest.mark.parametrize("text", ["", "1", "1.", "a.b", "1.2.", "1.2.3.4", "-1.2", "\u0661.\u0662"])
def test_parse_rejects_malformed_versions(text: str) -> None:
    with pytest.raises(ValueError, match="is not a schedr version"):
        SchedrVersion.parse(text)


def test_current_is_the_installed_package_version() -> None:
    assert SchedrVersion.current() == SchedrVersion.parse(version("schedr"))


@pytest.mark.parametrize(
    ("reader", "produced", "expected"),
    [
        (SchedrVersion(1, 4), SchedrVersion(1, 4), True),
        (SchedrVersion(1, 5), SchedrVersion(1, 4), True),
        (SchedrVersion(1, 4), SchedrVersion(1, 5), False),
        (SchedrVersion(2, 0), SchedrVersion(1, 9), False),
    ],
)
def test_can_read_same_major_and_equal_or_lower_minor(
    reader: SchedrVersion, produced: SchedrVersion, expected: bool
) -> None:
    assert reader.can_read(produced) is expected


def test_str_is_major_minor() -> None:
    assert str(SchedrVersion(1, 6)) == "1.6"


def test_versions_order_by_major_then_minor() -> None:
    assert SchedrVersion(1, 9) < SchedrVersion(2, 0) < SchedrVersion(2, 1)
