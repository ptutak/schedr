"""Pydantic models as outputs: an outcome union of models, a model whole, a field, a date."""

from datetime import UTC, datetime
from typing import Annotated

from pydantic import BaseModel, Field, HttpUrl

from schedr import Dag, RunIf, task
from schedr.pydantic import Model


class Settings(BaseModel):
    """Ordinary data: a plain `BaseModel`."""

    retries: Annotated[int, Field(ge=0)]
    region: str = "eu"


class Found(Model):
    """An outcome: derives from `schedr.pydantic.Model`."""

    url: HttpUrl
    fetched_at: datetime


class Missing(Model):
    """The other outcome."""

    reason: str


with Dag("pydantic_models", trigger=None):

    @task
    def settings() -> Settings:
        return Settings(retries=3)

    @task
    def locate(config: Settings) -> Found | Missing:
        url = HttpUrl(f"https://data.example.com/{config.region}/latest.csv")
        return Found(url=url, fetched_at=datetime(2026, 10, 29, 3, tzinfo=UTC))

    @task(run_if=RunIf.ALL_SUCCEEDED)  # skipped with `Missing`, so never given None
    def download(found: Found, retries: int) -> str:
        print(f"downloading {found.url} with {retries} retries")
        return str(found.url)

    @task
    def report(missing: Missing) -> None:
        print(missing.reason)

    @task
    def stamp(fetched_at: datetime) -> str:  # a model's date, as a plain datetime
        return f"{fetched_at:%Y-%m-%d %H:%M %Z}"

    config = settings()
    located = locate(config)
    download(located.on(Found), config.retries)
    report(located.on(Missing))
    stamp(located.on(Found).fetched_at)
