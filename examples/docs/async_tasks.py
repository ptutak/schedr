"""Async task functions: concurrent waits inside one attempt."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta

from schedr import Dag, acleanup_handle, connection, task, task_ctx


@dataclass
class Export:
    """What the attempt started on the warehouse."""

    job_id: str


@connection(base_url="https://api.example.com")
async def async_orders_api(base_url: str) -> AsyncIterator[str]:
    """Stands in for an async client, opened with `async with`."""
    await asyncio.sleep(0)
    yield base_url


async def fetch_page(base_url: str, page: int) -> int:
    """Stands in for one HTTP call."""
    await asyncio.sleep(0.01)
    return len(base_url) + page


async def cancel_export(handles: list[Export]) -> None:
    """A cleanup can be async too."""
    for handle in handles:
        print(f"cancelling {handle.job_id}")


with Dag("async_orders", description="Fetches orders with concurrent async waits inside one task."):

    @task(
        connections=[async_orders_api],
        cleanup=cancel_export,
        cleanup_timeout=timedelta(seconds=20),
    )
    async def fetch_all() -> int:
        await acleanup_handle(Export("export-1"))
        async with async_orders_api() as base_url:
            pages = await asyncio.gather(*(fetch_page(base_url, p) for p in range(10)))
        return sum(pages)

    @task(secrets=["etl/api#token"])
    async def report(total: int) -> None:
        token = await task_ctx.asecret("etl/api", "token")
        print(f"{total} rows, with a token of {len(token)} characters")

    report(fetch_all())
