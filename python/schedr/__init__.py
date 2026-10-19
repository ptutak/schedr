"""Python API for the schedr scheduler."""

from importlib.metadata import version

from schedr._greet import greet

__all__ = ["greet"]
__version__ = version("schedr")
