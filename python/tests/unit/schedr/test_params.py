"""Run parameters (``Dag(params=...)``, decisions 132-138): declaration, references, values."""

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

import pytest
from pydantic import BaseModel, Field

from schedr import Dag, dag_ctx, task
from schedr._model import ParamRef, registry
from schedr.context import RunValues, running
from schedr.runner.task import resolve
from schedr.testing import task_context
from schedr.triggers import Cron
from schedr.types import ProcessResult


@dataclass
class ReportParams:
    region: Literal["eu", "us"] = "eu"
    full: bool = False
    day: date | None = None
    limits: list[int] = field(default_factory=list)
    window: timedelta = timedelta(hours=1)
    pair: tuple[str, int] = ("a", 1)


@dataclass
class Required:
    name: str
    region: str = "eu"


class ModelParams(BaseModel):
    name: str = Field(min_length=1)
    tags: list[str] = Field(default_factory=lambda: ["x"])
    since: date = date(2026, 1, 1)


@task
def extract(region: str, day: date | None, limits: list[int]) -> int:
    return len(limits)


@task
def everything(p: ReportParams) -> str:
    return p.region


@task
def per_day(day: int) -> int:
    return day


@pytest.fixture(autouse=True)
def forget_dags() -> Iterator[None]:
    yield
    registry.take_entries()


def params_json(dag: Dag) -> dict[str, Any]:
    assert dag.model is not None
    result: dict[str, Any] = dag.model.to_json()["params"]
    return result


def test_a_dataclass_is_declared_with_its_type_a_json_schema_and_defaults() -> None:
    with Dag("report", trigger=Cron("0 3 * * *"), params=ReportParams) as dag:
        pass
    declared = params_json(dag)
    assert declared["type"]["type"] == "dataclass"
    assert declared["defaults"] == {
        "region": "eu", "full": False, "day": None, "limits": [],
        "window": "P0DT3600.000000S", "pair": ["a", 1],
    }  # fmt: skip
    properties = declared["schema"]["properties"]
    assert properties["region"] == {"enum": ["eu", "us"], "default": "eu"}
    assert properties["day"]["anyOf"] == [{"type": "string", "format": "date"}, {"type": "null"}]
    assert properties["window"] == {"type": "string", "default": "P0DT3600.000000S"}
    assert properties["pair"]["prefixItems"] == [{"type": "string"}, {"type": "integer"}]
    assert declared["schema"]["additionalProperties"] is False
    assert declared["schema"]["required"] == []


def test_a_pydantic_model_is_declared_with_its_own_schema_and_defaults() -> None:
    with Dag("m", params=ModelParams) as dag:
        pass
    declared = params_json(dag)
    assert declared["type"]["type"] == "model"
    assert declared["schema"]["properties"]["name"]["minLength"] == 1
    assert declared["schema"]["required"] == ["name"]
    assert declared["defaults"] == {"tags": ["x"], "since": "2026-01-01"}


def test_a_dag_without_params_writes_none() -> None:
    with Dag("plain") as dag:
        pass
    assert dag.model is not None
    assert "params" not in dag.model.to_json()


@dataclass
class _Result:
    result: ProcessResult


@dataclass
class WithResult:
    inner: _Result


@pytest.mark.parametrize(
    ("params", "trigger", "message"),
    [
        (Required, Cron("0 3 * * *"), "`Required.name` has no default, but the DAG has a trigger"),
        (ModelParams, Cron("0 3 * * *"), "`ModelParams.name` has no default"),
        (dict, None, "must be a dataclass or a Pydantic model class"),
        (ReportParams(), None, "must be a dataclass or a Pydantic model class"),
        (
            WithResult,
            None,
            r"DAG `bad`: params: field `inner`: field `result`: `process_result` can't be a "
            r"parameter",
        ),
    ],
)
def test_bad_declarations_are_refused(params: Any, trigger: Cron | None, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        Dag("bad", trigger=trigger, params=params)


def test_required_fields_are_fine_without_a_schedule() -> None:
    with Dag("manual", params=Required) as dag:
        pass
    assert params_json(dag)["schema"]["required"] == ["name"]


def test_references_in_the_dag_body_are_checked_against_the_class() -> None:
    with Dag("report", params=ReportParams) as dag:
        extract(region=dag_ctx.params.region, day=dag_ctx.params.day, limits=dag_ctx.params.limits)
        everything(dag_ctx.params(ReportParams))
    assert dag.model is not None
    tasks = dag.model.to_json()["tasks"]
    assert tasks[0]["arguments"]["region"] == {"ref": {"kind": "param", "path": ["region"]}}
    assert tasks[1]["arguments"]["p"] == {"ref": {"kind": "param"}}


@pytest.mark.parametrize(
    ("use", "message"),
    [
        (lambda: dag_ctx.params.nope, "`dag_ctx.params.nope` doesn't exist"),
        (lambda: dag_ctx.params.pair[2], r"`dag_ctx.params.pair\[2\]` doesn't exist"),
        (lambda: dag_ctx.params(Required), "the DAG's params are `ReportParams`"),
        (lambda: bool(dag_ctx.params.full), "can't be used as a value"),
        (lambda: dag_ctx.params.region == "eu", "can't be used as a value"),
        (
            lambda: extract(region=dag_ctx.params.full, day=None, limits=[]),
            "`dag_ctx.params.full` is `bool`",
        ),
        (lambda: per_day.expand(day=dag_ctx.params.limits), "can't be mapped over the run's"),
    ],
)
def test_misused_references_are_refused_at_parse_time(use: Any, message: str) -> None:
    with pytest.raises(TypeError, match=message), Dag("report", params=ReportParams):
        use()


def test_a_dag_without_params_refuses_dag_ctx_params() -> None:
    with pytest.raises(TypeError, match="declares no params"), Dag("plain"):
        _ = dag_ctx.params.region


def test_dag_ctx_params_outside_a_dag_or_task_is_an_error() -> None:
    with pytest.raises(RuntimeError, match="must be run inside a schedr task"):
        _ = dag_ctx.params


PLANNED = datetime(2026, 3, 4, tzinfo=UTC)
STORED = {
    "region": "us", "full": True, "day": {"$date": "2026-10-01"}, "limits": [1, 2],
    "window": {"$timedelta": 90_000_000}, "pair": {"$tuple": ["b", 2]},
}  # fmt: skip


def test_in_a_running_task_params_are_values_of_the_declared_class() -> None:
    values = RunValues("report", "r", "manual", PLANNED, PLANNED, STORED, ReportParams)
    with running(values):
        assert dag_ctx.params.region == "us"
        assert dag_ctx.params["day"] == date(2026, 10, 1)
        assert dag_ctx.params(ReportParams) == ReportParams(
            "us", True, date(2026, 10, 1), [1, 2], timedelta(seconds=90), ("b", 2)
        )
        with pytest.raises(TypeError, match="the DAG's params are `ReportParams`"):
            dag_ctx.params(Required)
        with pytest.raises(AttributeError):
            _ = dag_ctx.params.nope


def test_values_that_dont_fit_the_class_any_more_say_so() -> None:
    values = RunValues("report", "r", "manual", PLANNED, PLANNED, {"gone": 1}, ReportParams)
    with running(values), pytest.raises(TypeError, match="don't fit `ReportParams`"):
        dag_ctx.params(ReportParams)


def test_a_run_of_a_dag_without_params_says_it_has_none() -> None:
    values = RunValues("plain", "r", "manual", PLANNED, PLANNED)
    with running(values), pytest.raises(TypeError, match="declares no params"):
        _ = dag_ctx.params.region


def test_task_context_sets_params_from_an_instance() -> None:
    given = ReportParams(region="us", day=date(2026, 10, 1))
    with task_context(params=given):
        assert dag_ctx.params.region == "us"
        assert dag_ctx.params(ReportParams) == given
    with task_context(params=ModelParams(name="n")):
        assert dag_ctx.params(ModelParams).name == "n"


def test_the_runner_resolves_a_param_reference_from_the_run() -> None:
    run = {"params": STORED}
    assert resolve(ParamRef(("day",)).to_json(), {}, run) == {"$date": "2026-10-01"}
    assert resolve(ParamRef(("pair", 1)).to_json(), {}, run) == 2
    assert resolve(ParamRef().to_json(), {}, run) == STORED


@dataclass
class _SecretDefault:
    db_password: Literal["allowed"] = "hunter2"  # type: ignore[assignment]  # pyright: ignore[reportAssignmentType]


def test_a_default_that_doesnt_fit_is_refused_without_its_value() -> None:
    with pytest.raises(TypeError) as raised:
        Dag("bad", params=_SecretDefault)
    message = str(raised.value)
    assert "hunter2" not in message
    assert (
        message == "DAG `bad`: params: the default of `db_password` doesn't fit its declared type"
    )


@dataclass
class _HugeLiteral:
    db_password: Literal[18446744073709551616] = 18446744073709551616


def test_a_default_refused_isnt_shown_through_its_type() -> None:
    with pytest.raises(TypeError) as raised:
        Dag("bad", params=_HugeLiteral)
    assert "18446744073709551616" not in str(raised.value)


class _MistypedModel(BaseModel):
    db_password: int = "hunter2"  # type: ignore[assignment]  # pyright: ignore[reportAssignmentType]


def test_a_mistyped_model_default_warns_nothing(recwarn: pytest.WarningsRecorder) -> None:
    with Dag("model_default", params=_MistypedModel) as dag:
        pass
    assert [str(w.message) for w in recwarn if "hunter2" in str(w.message)] == []
    assert dag.model is not None
