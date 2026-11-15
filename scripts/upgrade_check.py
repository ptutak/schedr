#!/usr/bin/env python3
"""The upgrade check: a database the previous release filled, migrated by this tree's binary.

From 1.0.0 on, a schema change is a new migration (decision 456), so a database a released
schedr made and used must migrate to the current schema and go on working (decision 498: run
before every release, in CI on every push). The check:

1. installs the newest release at or below the tree's version from PyPI (the one users upgrade
   from) into a throwaway environment, and takes that release's own documentation examples
   (`examples/docs` at its tag) as the DAGs;
2. has that release fill its database the way users do, every run succeeding: secrets set,
   runs of the cleanup, notification, secret and run-of examples, the poke, named-event and
   run-of triggers switched on, `paid` events posted, a backfill, a wait step whose host is
   killed for blocking (a strike); then **every table holds at least one row** (the coverage
   gate: a table a later release adds fails the check until something here fills it), but
   those `NOT_FILLED` names with the reason;
3. checks this tree's `schedr db status` finds the schema behind or up to date;
4. migrates it with this tree's `schedr db migrate` alone, and compares: **every old row is
   still there, its old columns' values the same** (but in the tables `NOT_KEPT` names, with
   the reason), and the schema is up to date;
5. starts this tree's `schedr standalone` (not migrating) on the same database and reads it
   back: the old runs, the event and the secrets still there, and a new run succeeds.

    python scripts/upgrade_check.py --binary target/debug/schedr [--database URL]
    ... --from 1.0.0   a given release, not the newest at or below the tree's version
    ... --keep         the work folder (the logs, the database) kept and named

Run it with the project's Python (`uv run`): it is the one this tree's tasks run with, and it
has `psycopg` for `--database`, an empty Postgres database (`make upgrade-check` and CI make
one). Without it the check runs on a SQLite file in its work folder. The tags must be fetched
(`git fetch --tags`): the old release's examples come from its tag.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import shutil
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYPI = "https://pypi.org/pypi/schedr/json"
FINAL = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
STARTUP = 180.0
RUN_LIMIT = 180.0
FILL_LIMIT = 300.0

# The check's own DAGs, valid on every release from 1.0.0 on: one run to read back after the
# migration, and a wait step blocking its host's loop past `block_limit` (a strike).
OWN_DAGS = {
    "upgrade_check.py": '''"""The upgrade check's run (scripts/upgrade_check.py)."""

from schedr import CommandOperator, Dag, task

with Dag("upgrade_check"):

    @task
    def count() -> int:
        return 3

    CommandOperator("show", command=["echo", t"counted {count()}"])
''',
    "upgrade_notify.py": '''"""Notifications: one on the run's success, one a task sends itself."""

from schedr import Dag, Notify, api, task


@task
def done() -> str:
    api.notify("ops-slack", text="sent by a task")
    return "done"


with Dag("upgrade_notify", notify={"ops-slack": [Notify.RUN_SUCCEEDED]}):
    done()
''',
    "upgrade_strikes.py": '''"""A wait step whose host is killed for blocking its loop."""

import time
from datetime import timedelta

from schedr import NOT_YET, Dag, NotYet, sensor


@sensor.poke(every=timedelta(seconds=1), max_wait=timedelta(minutes=5))
async def blocks() -> str | NotYet:
    time.sleep(10)  # no await: the loop is blocked past `block_limit`, the host killed
    return NOT_YET


with Dag("upgrade_strikes"):
    blocks()
''',
}
# The examples' secrets, as `make standalone` sets them.
SECRETS = {
    "etl/db": "password=s3cret-db-password",
    "etl/api": "token=s3cret-api-token",
    "tenants/acme": "api_key=s3cret-acme-key",
}
# Runs triggered by hand: the check's own, then the old examples filling their tables.
TRIGGERED = ["upgrade_check", "upgrade_notify", "cleanup", "notifications_demo", "secrets_demo"]
TRIGGERED += ["orders"]
# Once `orders` has ended: they wait for its run (`wait_for` and an `@sensor` on `RunOf`).
AFTER_ORDERS = ["report", "checked_report"]
TRIGGERED_NO_WAIT = ["upgrade_strikes"]
# Switched on: a poke trigger (its events, its progress), the named-event and run-of ones.
# `inspect_packing`'s sensor says `NOT_YET` to a `packed` event without data: `sensor_passed`.
UNPAUSED = ["event_triggers", "inspect_payment", "ship_paid", "tally_orders", "inspect_packing"]
# Tables the old release can't fill from here, with the reason; none yet.
NOT_FILLED: dict[str, str] = {}
# Tables a migration may take rows from or change the old columns of, with the reason.
NOT_KEPT: dict[str, str] = {
    "master_lease": "`db migrate` holds the master's lease while it migrates: a new holder",
}

# A table's column names and its rows, each value in a form that compares across reads.
type Table = tuple[list[str], list[tuple[object, ...]]]


def version_key(version: str) -> tuple[int, int, int] | None:
    """`(major, minor, patch)` of a final release (`1.0.0`); `None` for a pre-release."""
    found = FINAL.match(version)
    if found is None:
        return None
    major, minor, patch = (int(part) for part in found.groups())
    return major, minor, patch


def tree_version(cargo_toml: str) -> str:
    """The `[workspace.package]` version, an rc's `-rcN` dropped (`1.1.0-rc1` -> `1.1.0`)."""
    found = re.search(
        r'^\[workspace\.package\][^\[]*?^version = "([^"]+)"', cargo_toml, re.MULTILINE | re.DOTALL
    )
    if found is None:
        raise ValueError("Cargo.toml has no [workspace.package] version")
    return found.group(1).split("-", 1)[0]


def previous_release(released: Sequence[str], current: str) -> str:
    """The newest final release at or below `current`: the one users upgrade from.

    The tree's version moves only at a release, so on main between releases it is the last
    release's (its new migrations tested from it), and at a release commit the one before.
    """
    now = version_key(current)
    if now is None:
        raise ValueError(f"`{current}` is not a final version")
    older = [v for v in released if (key := version_key(v)) is not None and key <= now]
    if not older:
        raise ValueError(f"no release at or below {current} among {sorted(released)}")
    return max(older, key=lambda v: version_key(v) or (0, 0, 0))


def missing_tables(counts: dict[str, int], not_filled: dict[str, str]) -> list[str]:
    """The tables with no row, but those `not_filled` names (sqlx's own table left out)."""
    return sorted(
        name
        for name, rows in counts.items()
        if rows == 0 and name not in not_filled and not name.startswith("_sqlx")
    )


def lost_rows(
    before: dict[str, Table], after: dict[str, Table], not_kept: dict[str, str]
) -> dict[str, str]:
    """What each table lost, but those `not_kept` names: itself, old columns, or old rows.

    Every row before must be there after with the same values in the old columns; rows added
    and new columns are fine (a migration's own bookkeeping, a column with its default).
    """
    lost: dict[str, str] = {}
    for name, (columns, rows) in sorted(before.items()):
        if name in not_kept:
            continue
        if name not in after:
            lost[name] = "the table is gone"
            continue
        new_columns, new_rows = after[name]
        if gone := [c for c in columns if c not in new_columns]:
            lost[name] = f"columns gone: {', '.join(gone)}"
            continue
        at = [new_columns.index(c) for c in columns]
        left = Counter(tuple(row[i] for i in at) for row in new_rows)
        missing = 0
        for row in rows:
            if left[row] > 0:
                left[row] -= 1
            else:
                missing += 1
        if missing:
            lost[name] = f"{missing} of {len(rows)} rows gone or changed"
    return lost


def released_versions() -> list[str]:
    """The versions PyPI lists for schedr."""
    with urllib.request.urlopen(PYPI, timeout=30) as answer:
        data: dict[str, object] = json.load(answer)
    releases = data.get("releases")
    return sorted(releases) if isinstance(releases, dict) else []


def comparable(value: object) -> object:
    """`value` as it compares and hashes: JSON (Postgres's `jsonb`) as sorted text."""
    if isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, memoryview):
        return bytes(value)
    return value


def table_rows(url: str) -> dict[str, Table]:
    """Every table of the database at `url`: its columns and rows."""
    if url.startswith("sqlite:"):
        path = re.sub(r"^sqlite:(//)?", "", url).split("?", 1)[0]
        with contextlib.closing(sqlite3.connect(path)) as db:
            names = [
                r[0]
                for r in db.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                )
            ]
            tables: dict[str, Table] = {}
            for name in names:
                cursor = db.execute(f'SELECT * FROM "{name}"')
                columns = [d[0] for d in cursor.description]
                tables[name] = (columns, [tuple(map(comparable, r)) for r in cursor.fetchall()])
            return tables
    import psycopg  # noqa: PLC0415 - only for Postgres, from the project's environment

    with psycopg.connect(url) as db:
        names = [
            r[0]
            for r in db.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()"
            ).fetchall()
        ]
        tables = {}
        for name in names:
            found = db.execute(f'SELECT * FROM "{name}"')
            columns = [d.name for d in found.description or []]
            tables[name] = (columns, [tuple(map(comparable, r)) for r in found.fetchall()])
        return tables


def table_counts(url: str) -> dict[str, int]:
    """Each table's row count in the database at `url`."""
    return {name: len(rows) for name, (_, rows) in table_rows(url).items()}


def free_port() -> int:
    """A loopback port nothing listens on now."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
    return port


def say(text: str) -> None:
    """One line of progress."""
    print(f"upgrade_check: {text}", flush=True)


def run(command: Sequence[str | Path], env: dict[str, str] | None = None) -> str:
    """`command`'s standard output; an error with both outputs if it fails."""
    done = subprocess.run(
        [str(part) for part in command], env=env, capture_output=True, text=True, check=False
    )
    if done.returncode != 0:
        raise RuntimeError(f"{' '.join(map(str, command))} failed:\n{done.stdout}{done.stderr}")
    return done.stdout


def answers(url: str) -> bool:
    """Whether `GET url` answers 200."""
    try:
        with urllib.request.urlopen(url, timeout=5) as answer:
            return bool(answer.status == 200)  # noqa: PLR2004 - HTTP OK
    except OSError:
        return False


def wait_for(ready: Callable[[], bool], limit: float, what: str, log: Path) -> None:
    """Polls `ready()` every second until it is true; an error naming `what` after `limit`."""
    deadline = time.monotonic() + limit
    while not ready():
        if time.monotonic() > deadline:
            raise RuntimeError(f"{what} not there after {limit:.0f} s; the log:\n{log.read_text()}")
        time.sleep(1)


@contextlib.contextmanager
def standalone(binary: Path, env: dict[str, str], log: Path, *, migrate: bool) -> Iterator[str]:
    """`binary standalone` until the block ends (SIGINT, as Ctrl-C); its API's URL.

    In a process group of its own: if it doesn't stop in a minute, the group is killed, its
    tasks too, before the work folder goes.
    """
    ports = {
        "SCHEDR_CONFIG__WEBSERVER__PORT": free_port(),
        "SCHEDR_CONFIG__MASTER__GRPC_PORT": free_port(),
        "SCHEDR_CONFIG__WORKER__LOG_PORT": free_port(),
        "SCHEDR_CONFIG__OPS__PORT": free_port(),
    }
    env = {**env, **{key: str(port) for key, port in ports.items()}}
    api = f"http://127.0.0.1:{ports['SCHEDR_CONFIG__WEBSERVER__PORT']}"
    with log.open("w") as out:
        process = subprocess.Popen(
            [str(binary), "standalone", *(["--migrate"] if migrate else []), "--no-ui"],
            env=env,
            stdout=out,
            stderr=out,
            start_new_session=True,
        )
        try:
            wait_for(lambda: answers(f"{api}/api/v1/dags"), STARTUP, f"{binary}'s API", log)
            yield api
        finally:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()


class Cli:
    """A `schedr` CLI against one standalone's API."""

    def __init__(self, binary: Path, api: str, env: dict[str, str], log: Path) -> None:
        """`binary` against `api`, with `env`; `log` is the standalone's, shown on failure."""
        self.binary, self.api, self.env, self.log = binary, api, env, log

    def __call__(self, *args: str) -> str:
        """`schedr ARGS --server API`'s output."""
        return run([self.binary, *args, "--server", self.api], env=self.env)

    def json(self, *args: str) -> dict[str, object]:
        """The command's `--json` answer, an object."""
        found = json.loads(self(*args, "--json"))
        return found if isinstance(found, dict) else {}

    def runs(self, dag: str) -> list[dict[str, object]]:
        """The DAG's runs, as `runs list --json` gives them."""
        found = self.json("runs", "list", dag).get("runs", [])
        return [r for r in found if isinstance(r, dict)] if isinstance(found, list) else []

    def wait_for_dags(self, dags: Sequence[str]) -> None:
        """Waits until `dags list` shows every one of `dags` (parsed)."""

        def listed() -> bool:
            text = self("dags", "list", "--json")
            return all(f'"{dag}"' in text for dag in dags)

        wait_for(listed, STARTUP, f"the DAGs {', '.join(dags)}", self.log)

    def trigger(self, dag: str) -> str:
        """Triggers a manual run of `dag`; its ID."""
        result = self.json("runs", "trigger", dag).get("result")
        run_id = str(result.get("run_id", "")) if isinstance(result, dict) else ""
        if not run_id:
            raise RuntimeError(f"no run ID in the answer to triggering {dag}")
        return run_id

    def state(self, dag: str, run_id: str) -> str:
        """The run's state, empty if it isn't listed."""
        return next((str(r.get("state")) for r in self.runs(dag) if r.get("run_id") == run_id), "")

    def wait_for_end(self, dag: str, run_id: str) -> str:
        """Waits for the run to end; its final state."""
        ended = {"success", "failed", "cancelled"}
        wait_for(lambda: self.state(dag, run_id) in ended, RUN_LIMIT, f"{dag}'s end", self.log)
        return self.state(dag, run_id)


def old_examples(release: str, into: Path) -> Path:
    """The release's `examples/docs` and `examples/standalone.toml`, from its tag, in `into`."""
    archive = subprocess.run(
        [
            "git",
            "-C",
            str(ROOT),
            "archive",
            f"v{release}",
            "examples/docs",
            "examples/standalone.toml",
        ],
        capture_output=True,
        check=False,
    )
    if archive.returncode != 0:
        raise RuntimeError(
            f"no tag v{release} here (`git fetch --tags`): {archive.stderr.decode().strip()}"
        )
    with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
        tar.extractall(into, filter="data")
    return into / "examples"


def fill(cli: Cli, url: str) -> list[str]:
    """The old release's part: every table filled; the IDs of the runs it ended with success."""
    cli.wait_for_dags([*TRIGGERED, *AFTER_ORDERS, *TRIGGERED_NO_WAIT, *UNPAUSED])
    for dag in UNPAUSED:
        cli("dags", "unpause", dag)
    started = {dag: cli.trigger(dag) for dag in [*TRIGGERED, *TRIGGERED_NO_WAIT]}
    cli("events", "post", "paid", "--key", "order-1", "--data", '{"amount": 5}')
    cli("runs", "backfill", "nightly", "--from", "2026-11-01T00:00", "--to", "2026-11-02T12:00")
    # Posted while `inspect_packing`'s sensor waits for it (3 s, from its start); without
    # data, so the sensor says `NOT_YET` to it.
    wait_for(
        lambda: any(r.get("state") == "running" for r in cli.runs("inspect_packing")),
        RUN_LIMIT,
        "inspect_packing's run",
        cli.log,
    )
    cli("events", "post", "packed", "--key", "order-1")
    ended = {dag: cli.wait_for_end(dag, started[dag]) for dag in TRIGGERED}
    started |= {dag: cli.trigger(dag) for dag in AFTER_ORDERS}
    ended |= {dag: cli.wait_for_end(dag, started[dag]) for dag in AFTER_ORDERS}
    say(f"the old release's runs: {', '.join(f'{d} {s}' for d, s in ended.items())}")
    if failed := {dag: state for dag, state in ended.items() if state != "success"}:
        raise RuntimeError(f"the old release's runs didn't succeed: {failed}")

    def filled() -> bool:
        return not missing_tables(table_counts(url), NOT_FILLED)

    try:
        wait_for(filled, FILL_LIMIT, "a row in every table", cli.log)
    except RuntimeError:
        empty = missing_tables(table_counts(url), NOT_FILLED)
        raise RuntimeError(
            f"the old release left these tables empty: {', '.join(empty)}. Make it fill them "
            "here, or name them in NOT_FILLED with the reason"
        ) from None
    return [started[dag] for dag in ended]


def check(binary: Path, python: Path, database: str | None, old: str, work: Path) -> None:
    """The whole check, in `work`; an error saying what failed."""
    say(f"from {old} (PyPI, its examples from v{old}) to this tree's {binary}")
    venv = work / "old"
    run(["uv", "venv", "--quiet", "--python", "3.14", venv])
    run(["uv", "pip", "install", "--quiet", "--python", venv / "bin" / "python", f"schedr=={old}"])
    examples = old_examples(old, work / "old-source")
    own = work / "upgrade"
    own.mkdir()
    for name, source in OWN_DAGS.items():
        (own / name).write_text(source)
    url = database or f"sqlite://{work / 'schedr.db'}?mode=rwc"
    base = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(work),
        "TZ": "UTC",
        "SCHEDR_CONFIG__DATABASE__URL": url,
        "SCHEDR_CONFIG_FILE": str(examples / "standalone.toml"),
        "SCHEDR_DAG_PATH": f"{examples / 'docs'}:{own}",
        "SCHEDR_CONFIG__WORKER__DATA_DIR": str(work / "data"),
        "SCHEDR_CONFIG__WATCHERS__BLOCK_LIMIT": "2s",
    }
    old_bin = venv / "bin" / "schedr"
    run([old_bin, "db", "migrate"], env=base)
    base["SCHEDR_SECRET_KEY"] = run([old_bin, "secrets", "new-key"], env=base).strip()
    for path, pair in SECRETS.items():
        run([old_bin, "secrets", "set", path, pair], env=base)
    old_env = {**base, "SCHEDR_CONFIG__INTERPRETERS__PYTHON__PATH": str(venv / "bin" / "python")}
    with standalone(old_bin, old_env, work / "old.log", migrate=True) as api:
        succeeded = fill(Cli(old_bin, api, old_env, work / "old.log"), url)
    before = table_rows(url)
    rows = sum(len(r) for _, r in before.values())
    say(f"{old} filled all {len(before)} tables ({rows} rows)")
    # Behind (migrations since the release) or up to date (none yet); anything else, such as
    # a schema this tree calls modified (B236), is an upgrade that can't work.
    status = run([binary, "db", "status"], env=base).strip()
    if not re.search(r": (up to date|outdated: \d+ migration\(s\) missing)", status):
        raise RuntimeError(f"this tree's `db status` on {old}'s database: {status}")
    say(f"this tree's db status: {status.split(': ', 1)[-1]}")
    # The migration alone, before this tree runs anything: then every old row must be there,
    # its old columns' values the same, and the schema up to date (the standalone below
    # doesn't migrate, so it can't make up for a migration left undone).
    run([binary, "db", "migrate", "--wait"], env=base)
    if lost := lost_rows(before, table_rows(url), NOT_KEPT):
        raise RuntimeError(f"the migration lost data: {lost}")
    status = run([binary, "db", "status"], env=base).strip()
    if not re.search(r": up to date", status):
        raise RuntimeError(f"this tree's `db status` after `db migrate`: {status}")
    say(f"migrated: every table kept its {rows} rows, their values the same")
    new_env = {**base, "SCHEDR_CONFIG__INTERPRETERS__PYTHON__PATH": str(python)}
    with standalone(binary, new_env, work / "new.log", migrate=False) as api:
        read_back(Cli(binary, api, new_env, work / "new.log"), succeeded)
    say("migrated and read back")


def read_back(cli: Cli, succeeded: list[str]) -> None:
    """This tree's part: what the old release stored is still there and still works."""
    runs = {
        str(r.get("run_id")): str(r.get("state"))
        for d in [*TRIGGERED, *AFTER_ORDERS]
        for r in cli.runs(d)
    }
    gone = [run_id for run_id in succeeded if runs.get(run_id) != "success"]
    if gone:
        raise RuntimeError(f"runs no longer shown as success after the migration: {gone}")
    if "order-1" not in cli("events", "list", "--name", "paid"):
        raise RuntimeError("the `paid` event posted before the migration is gone")
    if "s3cret-db-password" not in run([cli.binary, "secrets", "get", "etl/db"], env=cli.env):
        raise RuntimeError("the secret etl/db no longer reads back")
    run_id = cli.trigger("upgrade_check")
    if (state := cli.wait_for_end("upgrade_check", run_id)) != "success":
        raise RuntimeError(f"a new run on the migrated database ended {state}")
    say(f"{len(succeeded)} old runs, the event and the secret read back; {run_id} ran")


def remove(work: Path) -> None:
    """Deletes the work folder, schedr's read-only code copies in it too."""

    def writable_then_again(function: object, path: str, _error: BaseException) -> None:
        assert callable(function)
        Path(path).parent.chmod(stat.S_IRWXU)
        Path(path).chmod(stat.S_IRWXU)
        function(path)

    shutil.rmtree(work, onexc=writable_then_again)


def main(argv: list[str] | None = None) -> int:
    """Runs the check; 0 when it holds."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--binary", type=Path, required=True, help="this tree's schedr binary")
    parser.add_argument("--database", help="an empty Postgres database's URL (default: SQLite)")
    parser.add_argument("--from", dest="old", help="the release to upgrade from")
    parser.add_argument("--keep", action="store_true", help="keep the work folder and name it")
    args = parser.parse_args(argv)
    current = tree_version((ROOT / "Cargo.toml").read_text())
    old = args.old or previous_release(released_versions(), current)
    work = Path(tempfile.mkdtemp(prefix="schedr-upgrade-"))
    try:
        # This Python as it is: resolving a virtualenv's link would leave its packages behind.
        check(args.binary.resolve(), Path(sys.executable).absolute(), args.database, old, work)
    except RuntimeError as error:
        print(f"upgrade_check: FAILED: {error}", file=sys.stderr)
        return 1
    finally:
        if args.keep:
            say(f"the work folder: {work}")
        else:
            remove(work)
    return 0


if __name__ == "__main__":
    sys.exit(main())
