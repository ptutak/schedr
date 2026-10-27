"""Checks that the chart's default preload list is the one the image's build checks.

`worker.forkServer.preload` in the chart's values.yaml must list the modules of
images/schedr/preload.txt, in order: the image's build fails if that list leaves a thread
running, so a chart list of its own wouldn't be checked.

Usage: python scripts/check-preload-list.py <values.yaml> <preload.txt>
"""

import sys
from pathlib import Path

import yaml


def preload_list(path: Path) -> list[str]:
    """The modules in a preload list file (one per line; `#` comments and blanks skipped)."""
    lines = (line.strip() for line in path.read_text().splitlines())
    return [line for line in lines if line and not line.startswith("#")]


def main(values: Path, listed: Path) -> int:
    """Compares the two lists; prints the difference and returns 1 when they differ."""
    chart = yaml.safe_load(values.read_text())["worker"]["forkServer"]["preload"]
    image = preload_list(listed)
    if chart == image:
        return 0
    print(
        f"{values}: worker.forkServer.preload is {chart}, but {listed} (checked by the "
        f"image's build) is {image}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main(Path(sys.argv[1]), Path(sys.argv[2])))
