"""A DAG taking run parameters, for the trigger form's browser test (M4.5c)."""

from dataclasses import dataclass, field
from datetime import date
from typing import Literal

from schedr import Dag, dag_ctx, task


@dataclass
class FormParams:
    """One field of each kind the form shows, a secret one among them."""

    region: str = "eu"
    rows: int = 10
    full: bool = False
    day: date | None = None
    mode: Literal["fast", "slow"] = "fast"
    tags: list[str] = field(default_factory=lambda: ["a"])
    api_token: str = "hunter2"


@task
def show(params: FormParams) -> str:
    return f"{params.region} {params.rows}"


with Dag("form_params", params=FormParams):
    show(dag_ctx.params(FormParams))
