"""Tests of the example DAG metrics_load: its tasks as plain calls (a whole run takes a minute)."""

from metrics_load import few, items, tick, total


def test_its_tasks_are_plain_functions() -> None:
    assert len(items()) == 40
    assert few() == list(range(8))
    assert tick(3) == 9
    assert total([1, 4], [2, 3], [40, 60]) == "2 ticks, 5 spins, 100 MiB held"
