#!/usr/bin/env python3
"""Whether the integration tests' environment is set: nextest's setup step (.config/nextest.toml).

Usage: test_env.py VARIABLE; exits 1 with what to do when VARIABLE is unset (or names a Python
that isn't there), so a run stops once instead of failing every test that needs it.
`--check` runs its own small self-test.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

#: What each variable is, for the message.
NEEDS = {
    "SCHEDR_TEST_PYTHON": "the Python with schedr installed, e.g. $(pyenv prefix)/bin/python",
    "SCHEDR_TEST_POSTGRES_URL": "an administrative Postgres URL, e.g. postgres://postgres:pw@127.0.0.1:5432/postgres",
}


def problem(variable: str, environ: dict[str, str]) -> str | None:
    """What is wrong with `variable` in `environ`, or none."""
    value = environ.get(variable, "")
    if not value:
        return (
            f"{variable} is not set: {NEEDS[variable]}. Run the integration tests with "
            "`make test-integration` (it starts the services and sets it), or set it; unit "
            "tests alone: `cargo nextest run -E 'kind(lib)'`."
        )
    if variable == "SCHEDR_TEST_PYTHON" and not Path(value).is_file():
        return f"SCHEDR_TEST_PYTHON is {value}, which isn't a file: {NEEDS[variable]}."
    return None


def check() -> None:
    """A self-test: unset, set, a missing Python, an unknown variable."""
    assert "make test-integration" in (problem("SCHEDR_TEST_POSTGRES_URL", {}) or "")
    assert problem("SCHEDR_TEST_POSTGRES_URL", {"SCHEDR_TEST_POSTGRES_URL": "postgres://x"}) is None
    assert "isn't a file" in (
        problem("SCHEDR_TEST_PYTHON", {"SCHEDR_TEST_PYTHON": "/nowhere/python"}) or ""
    )
    assert problem("SCHEDR_TEST_PYTHON", {"SCHEDR_TEST_PYTHON": sys.executable}) is None


def main(argv: list[str]) -> int:
    """Check `argv[1]`, or run the self-test with `--check`."""
    if argv[1:] == ["--check"]:
        check()
        print("ok")
        return 0
    if len(argv) != 2 or argv[1] not in NEEDS:  # noqa: PLR2004 - the program and one variable
        print(f"usage: test_env.py {{{' | '.join(NEEDS)}}}", file=sys.stderr)
        return 2
    found = problem(argv[1], dict(os.environ))
    if found:
        print(f"integration tests: {found}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
