"""Connections: defined once, declared by the tasks that open them, rendered per environment."""

from collections.abc import Iterator
from dataclasses import dataclass

from schedr import Dag, connection, secrets, task


@dataclass
class ApiClient:
    """Stands in for a real client library's (httpx, pymongo, ...)."""

    base_url: str
    token: str
    timeout: float
    closed: bool = False

    def get(self, path: str) -> str:
        """What a real client would fetch; here, what it was asked."""
        return f"GET {self.base_url}{path} with a token of {len(self.token)} characters"


# Usually in a module of its own, imported by every DAG that uses it.
@connection(token=secrets["etl/api"]["token"], base_url="https://api.example.com")
def orders_api(token: str, base_url: str, timeout: float = 10.0) -> Iterator[ApiClient]:
    client = ApiClient(base_url, token, timeout)
    try:
        yield client  # what `with orders_api() as api:` gives
    finally:
        client.closed = True  # runs when the block ends, on errors too


@task(connections=[orders_api])
def fetch_orders() -> str:
    with orders_api() as api:
        return api.get("/orders")


with Dag(
    "connections_demo",
    description="Opens connections that are defined once and filled in per environment.",
):
    fetch_orders()
