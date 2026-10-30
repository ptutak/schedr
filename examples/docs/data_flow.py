"""Passing parts of an output, and ordering tasks that share no data."""

from dataclasses import dataclass

from schedr import CommandOperator, Dag, chain, task


@dataclass(frozen=True)
class Batch:
    """Rows fetched from one source."""

    rows: int
    source: str


with Dag("data_flow", trigger=None):

    @task
    def fetch() -> Batch:
        return Batch(2, "primary")

    @task
    def load(rows: int, source: str) -> str:
        text = f"{source}:{rows}"
        print(text)
        return text

    @task
    def left() -> None:
        print("left")

    @task
    def right() -> None:
        print("right")

    @task
    def finish(note: str) -> None:
        print(note)

    batch = fetch()
    note = load(batch.rows, batch.source)
    CommandOperator("show", command=["echo", note])
    finished = finish(note)
    chain(batch, [left(), right()], finished)
