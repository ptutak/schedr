"""The schedr version that produced stored data, as ``major.minor`` (``schedr-model``,
``version.rs``)."""

import re
from dataclasses import dataclass
from importlib.metadata import version
from typing import Self

_VERSION = re.compile(r"(?P<major>[0-9]+)\.(?P<minor>[0-9]+)(\.[^.]+)?")


@dataclass(frozen=True, slots=True, order=True)
class SchedrVersion:
    """A schedr version as ``major.minor``; the patch level never changes compatibility.

    Attributes:
        major: Data of another major version is never read.
        minor: A reader reads data of an equal or lower minor version.
    """

    major: int
    minor: int

    @classmethod
    def parse(cls, text: str) -> Self:
        """Parses ``major.minor`` or ``major.minor.patch`` (the patch is ignored).

        Args:
            text: The version string, e.g. ``"0.1"`` or ``"0.1.0"``.

        Returns:
            The version.

        Raises:
            ValueError: If the text isn't such a version.
        """
        match = _VERSION.fullmatch(text)
        if match is None:
            raise ValueError(f"`{text}` is not a schedr version (expected `major.minor`)")
        return cls(int(match["major"]), int(match["minor"]))

    @classmethod
    def current(cls) -> Self:
        """The version of the installed ``schedr`` package.

        Returns:
            The installed version, without its patch level.
        """
        return cls.parse(version("schedr"))

    def can_read(self, produced: SchedrVersion) -> bool:
        """Whether this version can read data produced by ``produced``.

        Args:
            produced: The version that produced the data.

        Returns:
            True for the same major and an equal or lower minor version.
        """
        return self.major == produced.major and produced.minor <= self.minor

    def __str__(self) -> str:
        """The version as ``major.minor``, the way it is stored."""
        return f"{self.major}.{self.minor}"
