"""Checks that the chart's default preload list is the one the image's build checks.

The chart as rendered (`helm template`, on stdin) must set the DAG interpreter's preload to the
modules of images/schedr/preload.txt, in order: the image's build fails if that list leaves a
thread running, so a chart list of its own wouldn't be checked. Only the standard library: the
chart's CI job has `helm` and `python3`, nothing else.

Usage: helm template t charts/schedr | python3 scripts/check-preload-list.py <preload.txt>
"""

import json
import re
import sys
from pathlib import Path

PRELOAD = re.compile(
    r"name: SCHEDR_CONFIG__INTERPRETERS__[A-Z0-9_]+__PRELOAD\s*\n\s*value: (\"(?:[^\"\\]|\\.)*\")"
)
"""The rendered preload variable and its value (a YAML double-quoted JSON list)."""


def preload_list(path: Path) -> list[str]:
    """The modules in a preload list file (one per line; `#` comments and blanks skipped)."""
    lines = (line.strip() for line in path.read_text().splitlines())
    return [line for line in lines if line and not line.startswith("#")]


def rendered_lists(manifest: str) -> list[list[str]]:
    """Every preload list the rendered chart sets (one per container that gets it)."""
    # The YAML double-quoted value is a JSON string, so json reads its escapes too.
    return [json.loads(json.loads(quoted)) for quoted in PRELOAD.findall(manifest)]


def main(manifest: str, listed: Path) -> int:
    """Compares the lists; prints the difference and returns 1 when they differ or none is set."""
    image = preload_list(listed)
    lists = rendered_lists(manifest)
    if not lists:
        print("the rendered chart sets no preload list (worker.forkServer off?)", file=sys.stderr)
        return 1
    wrong = [chart for chart in lists if chart != image]
    if not wrong:
        return 0
    print(
        f"the chart's worker.forkServer.preload is {wrong[0]}, but {listed} (checked by the "
        f"image's build) is {image}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.stdin.read(), Path(sys.argv[1])))
