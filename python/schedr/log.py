"""Structured log lines (decision 514).

Shared with extensions' plugins: they live in :mod:`schedr_extension._log`, which imports
nothing of ``schedr`` (B283).
"""

from schedr_extension._log import KEY, StructuredHandler, install, line

__all__ = ["KEY", "StructuredHandler", "install", "line"]
