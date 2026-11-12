"""Acting on the first of several: `first_of`, `at_least`, and joining the paths of a `@branch`."""

from schedr import Dag, Data, Task, at_least, branch, first_of, task


@task
def from_cache() -> str:
    """One way to get the prices."""
    return "cached prices"


@task
def from_service() -> str:
    """Another way to get the same prices."""
    return "fresh prices"


@task
def price(value: str, winner: str) -> str:
    """Runs as soon as one of them has its value; the other runs on."""
    return f"{value} from {winner}"


@task
def region(name: str) -> int:
    """Asks one region to accept the write."""
    return len(name)


@task
def accepted(sizes: list[int], winners: list[str]) -> str:
    """Runs as soon as two regions have answered, in the order they did."""
    return f"{len(sizes)} regions: {', '.join(sorted(winners))}"


class Report(Data):
    """Where a report went."""

    path: str


@task
def as_pdf() -> Report:
    """The path chosen."""
    return Report("report.pdf")


@task
def as_html() -> Report:
    """The path not chosen: it is skipped."""
    return Report("report.html")


@task
def publish(report: Report) -> str:
    """Gets the one value there is, not one parameter per path."""
    return f"published {report.path}"


with Dag(
    "first_of_demo",
    description="Goes on with the first of several sources to answer, and joins a branch's paths.",
):
    value, winner = first_of(from_cache(), from_service(), with_winner=True)
    price(value, winner)

    sizes, regions = at_least(
        2,
        region.override(task_id="eu")("eu"),
        region.override(task_id="us")("us"),
        region.override(task_id="asia")("asia"),
        with_winner=True,
    )
    accepted(sizes, regions)

    pdf = as_pdf()
    html = as_html()

    @branch
    def choose() -> list[Task]:
        return [pdf]

    choose() >> [pdf, html]
    publish(first_of(pdf, html, task_id="report"))
