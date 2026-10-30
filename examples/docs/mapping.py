"""Mapping: one task instance per element, lanes, and gathering the results."""

from collections.abc import Sequence

from schedr import Dag, Data, RunIf, task


class Rows(Data):
    """A file's rows."""

    count: int


class Missing(Data):
    """A file that isn't there."""

    reason: str


@task
def list_files() -> list[str]:
    return ["a.csv", "missing.csv", "orders.csv"]


@task
def fetch(bucket: str, path: str) -> Rows | Missing:
    # An expected problem is an outcome, not an exception: the instance succeeds with
    # `Missing`, and every gather downstream still runs. An uncaught exception would fail
    # the step and cancel the other instances.
    if path.startswith("missing"):
        return Missing(reason=f"{bucket}/{path} not found")
    return Rows(count=len(path))


@task
def count(rows: Rows) -> int:
    return rows.count


@task
def total(counts: Sequence[int | None]) -> int:
    # `count` was skipped where `fetch` ended `Missing`: `None` in its place. (A `Sequence`:
    # type checkers read `count.expand(...)` as a `list[int]`, not knowing which are skipped.)
    return sum(c for c in counts if c is not None)


@task
def count_any(result: Rows | Missing) -> int | None:
    # Deciding in the task, on the whole union: no instance is skipped.
    return result.count if isinstance(result, Rows) else None


@task
def report(results: list[Rows | Missing]) -> list[str]:
    # The whole union of every instance, in the order of the files.
    return [r.reason for r in results if isinstance(r, Missing)]


with Dag("mapped"):
    # One `fetch` per file, the bucket fixed for all of them.
    results = fetch.partial(bucket="raw").expand(path=list_files())

    # Lanes: `count[i]` starts as soon as `fetch[i]` ends `Rows`; for `Missing` it is
    # skipped. `total` (the gather) waits for all of them.
    total(count.expand(rows=results.on(Rows)))

    # All or nothing: with ALL_SUCCEEDED one skipped instance skips the whole task (and the
    # gather after it), so `strict_total` runs only if every file had rows.
    strict = count.override(task_id="strict_count", run_if=RunIf.ALL_SUCCEEDED)
    total.override(task_id="strict_total")(strict.expand(rows=results.on(Rows)))

    # Gather everything: map without narrowing, and decide in the task.
    total.override(task_id="total_any")(count_any.expand(result=results))
    report(results)
