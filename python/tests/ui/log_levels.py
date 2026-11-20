"""A DAG writing a plain line and structured lines at every level, for the Logs tab's level
filter (decision 514)."""

import json

from schedr import Dag, task


@task
def levels() -> None:
    """A plain line, then one per level (a hidden one's gap would show between them)."""
    print("a plain line")
    for level in ("debug", "info", "warning", "error"):
        print(json.dumps({"schedr_log": 1, "level": level, "message": f"a {level} line"}))


with Dag("log_levels"):
    levels()
