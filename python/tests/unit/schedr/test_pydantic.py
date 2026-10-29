import warnings
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Annotated

import pytest
from pydantic import BaseModel, Field, HttpUrl

from schedr import CommandOperator, Dag, TypingHint, task
from schedr._model import ModelType, registry
from schedr._serialization import (
    CodecError,
    assignment_problem,
    decode,
    describe,
    encode,
    encode_any,
    follow_path,
)
from schedr.outcomes import from_return_type
from schedr.pydantic import Model


@pytest.fixture(autouse=True)
def clean_registry() -> Iterator[None]:
    registry.take_defined()
    yield
    registry.take_defined()


class Remote(Model):
    url: str


class Missing(Model):
    reason: str


def test_a_model_is_a_pydantic_model() -> None:
    remote = Remote(url="x")
    assert isinstance(remote, BaseModel)
    assert remote.model_dump() == {"url": "x"}
    assert Remote.model_validate({"url": "y"}) == Remote(url="y")


def test_model_outcome_classes_get_no_typing_hint() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", TypingHint)
        assert from_return_type(Remote | Missing, "task `locate`").names == ("Remote", "Missing")


def test_a_plain_base_model_outcome_gets_a_hint_naming_model() -> None:
    class PlainRemote(BaseModel):
        url: str

    with pytest.warns(TypingHint, match=r"derive it from schedr\.pydantic\.Model"):
        from_return_type(PlainRemote | Missing, "task `locate`")


# --- outputs and inputs (decision 59) ---------------------------------------------------


class Settings(BaseModel):
    retries: Annotated[int, Field(ge=0)]
    region: str = "eu"


class Endpoint(BaseModel):
    url: HttpUrl
    code: Annotated[str, Field(pattern="^[a-z]+$")]


class Tree(BaseModel):
    name: str
    children: list[Tree] = []


def test_a_model_is_described_by_its_schema_on_each_side() -> None:
    out = describe(Settings)
    in_ = describe(Settings, "input")
    assert isinstance(out, ModelType)
    assert isinstance(in_, ModelType)
    assert out.name == "Settings"
    assert out.schema["properties"]["retries"] == {
        "minimum": 0,
        "title": "Retries",
        "type": "integer",
    }
    # Written: every field; read: those without a default.
    assert out.schema["required"] == ["retries", "region"]
    assert in_.schema["required"] == ["retries"]
    assert out.to_json()["type"] == "model"


def test_a_model_round_trips_through_pydantics_json() -> None:
    value = Endpoint(url=HttpUrl("https://example.com/x"), code="abc")
    stored = encode(value, Endpoint)
    assert stored == {"url": "https://example.com/x", "code": "abc"}
    assert decode(stored, Endpoint) == value
    assert encode_any(value) == stored
    with pytest.raises(CodecError, match="expected Endpoint, got Settings"):
        encode(Settings(retries=1), Endpoint)
    with pytest.raises(CodecError, match=r"(?s)Endpoint: .*should match pattern"):
        decode({"url": "https://example.com", "code": "ABC"}, Endpoint)


def _problem(source: object, target: object) -> str | None:
    return assignment_problem(describe(source), describe(target, "input"))


def test_the_same_or_a_tighter_type_fits() -> None:
    assert _problem(Settings, Settings) is None
    assert _problem(Tree, Tree) is None
    assert _problem(Settings | None, Settings | None) is None
    # A part of a model: `retries` is an int with minimum 0, so any int parameter takes it.
    retries = follow_path(describe(Settings), ("retries",))
    assert retries is not None
    assert assignment_problem(retries, describe(int, "input")) is None
    url = follow_path(describe(Endpoint), ("url",))
    assert url is not None
    assert assignment_problem(url, describe(str, "input")) is None


def test_what_cant_be_proven_to_fit_is_refused() -> None:
    plain_int = describe(int)
    needs_minimum = describe(dict[str, Settings], "input")
    retries_in = follow_path(needs_minimum, ("x", "retries"))
    assert retries_in is not None
    # 59.1: an int may be negative; the parameter needs minimum 0.
    assert assignment_problem(plain_int, retries_in) == (
        "the parameter needs minimum 0; the output has none"
    )
    # 59.1: a str isn't proven to be a URL.
    url_in = follow_path(describe(Endpoint, "input"), ("url",))
    assert url_in is not None
    assert (problem := assignment_problem(describe(str), url_in)) is not None
    assert problem.startswith("the parameter needs format `uri`; the output has none")
    # A model that may be None into one that may not.
    assert _problem(Settings | None, Settings) == (
        "the output may be None; the parameter takes `object`"
    )


def test_patterns_and_other_uncomparable_parts_must_be_identical() -> None:
    class Loose(BaseModel):
        code: Annotated[str, Field(pattern="^[a-z0-9]+$")]

    class Strict(BaseModel):
        code: Annotated[str, Field(pattern="^[a-z]+$")]

    assert _problem(Strict, Strict) is None
    assert _problem(Strict, Loose) == (
        "`code`: the parameter needs pattern `^[a-z0-9]+$`; the output has `^[a-z]+$` "
        "(patterns must be identical)"
    )


class Stamped(BaseModel):
    at: datetime
    day: date
    took: timedelta


def test_a_models_dates_pass_to_schedrs_and_back() -> None:
    # Parse time (decision 60): a model's date fits a `datetime` parameter, and back.
    at = follow_path(describe(Stamped), ("at",))
    assert at is not None
    assert assignment_problem(at, describe(datetime, "input")) is None
    stamped_in = follow_path(describe(dict[str, Stamped], "input"), ("x", "at"))
    assert stamped_in is not None
    assert assignment_problem(describe(datetime), stamped_in) is None
    # Run time: the task receives a value, by Pydantic's own reading.
    value = Stamped(
        at=datetime(2026, 10, 29, 3, tzinfo=UTC), day=date(2026, 10, 29), took=timedelta(hours=1)
    )
    stored = encode(value, Stamped)
    assert decode(stored["at"], datetime) == value.at
    assert decode(stored["day"], date) == value.day
    assert decode(stored["took"], timedelta) == value.took
    # schedr's tagged dates into a model: untagged, then validated.
    tagged = {
        "at": encode(value.at, datetime),
        "day": encode(value.day, date),
        "took": encode(value.took, timedelta),
    }
    assert decode(tagged, Stamped) == value


def test_a_naive_model_date_stays_naive() -> None:
    naive = Stamped(at=datetime(2026, 10, 29, 3), day=date(2026, 10, 29), took=timedelta(0))
    received = decode(encode(naive, Stamped)["at"], datetime)
    assert received == datetime(2026, 10, 29, 3)
    assert received.tzinfo is None
    # Stored again by schedr, it is refused, saying where it came from (decision 63).
    with pytest.raises(CodecError, match="declare it AwareDatetime, or add a zone"):
        encode(received, datetime)


def test_fields_a_model_needs_must_be_written_by_the_output() -> None:
    class Partial(BaseModel):
        retries: int

    class Needs(BaseModel):
        retries: int
        region: str

    assert _problem(Partial, Needs) == (
        "field `region` is missing in the output; the parameter needs it"
    )
    # Extra fields of the output are ignored by a model, as Pydantic ignores them.
    assert _problem(Needs, Partial) is None


def test_model_outputs_route_and_pass_in_a_dag() -> None:
    @task
    def locate() -> Remote | Missing:
        return Remote(url="s3://bucket/key")

    @task
    def download(where: Remote) -> str:
        return where.url

    @task
    def configure() -> Settings:
        return Settings(retries=1)

    @task
    def plan(retries: int) -> None: ...

    @task
    def bad(retries: Annotated[str, Field(min_length=1)]) -> None: ...

    with Dag("models"):
        download(locate().on(Remote))
        plan(configure().retries)
    with (
        Dag("refused"),
        pytest.raises(
            TypeError, match=r"argument `retries`: expects `str`, but `configure\.retries`"
        ),
    ):
        bad(configure().retries)


# --- Grok's review of 2026-10-29 -------------------------------------------------------


def test_a_models_nan_or_infinity_isnt_stored() -> None:
    class Num(BaseModel):
        x: float

    with pytest.raises(CodecError, match=r"at `\.x`: nan can't be stored"):
        encode(Num(x=float("nan")), Num)
    with pytest.raises(CodecError, match=r"at `\.x`: inf can't be stored"):
        encode_any(Num(x=float("inf")))


def test_outcome_on_a_model_with_an_outcome_field_is_refused() -> None:
    class Result(BaseModel):
        outcome: str

    @task
    def compute() -> Result:
        return Result(outcome="ok")

    @task
    def take(s: str) -> None: ...

    with (
        Dag("d"),
        pytest.raises(TypeError, match=r'write `compute\["outcome"\]` for the field'),
    ):
        take(compute().outcome)


def test_a_models_text_and_number_fields_render_in_commands() -> None:
    class Endpoint(BaseModel):
        url: str
        port: int
        tags: list[str]

    @task
    def endpoint() -> Endpoint:
        return Endpoint(url="x", port=1, tags=[])

    with Dag("d"):
        found = endpoint()
        CommandOperator("curl", ["curl", found.url, t"{found.port}"])
    with Dag("e"), pytest.raises(TypeError, match=r"`Endpoint\.tags` can't be rendered"):
        CommandOperator("bad", ["echo", endpoint().tags])


def test_a_container_that_cant_be_the_parameters_shape_is_refused() -> None:
    @task
    def name() -> str:
        return "a"

    @task
    def use(settings: Settings) -> None: ...

    with (
        Dag("d"),
        pytest.raises(TypeError, match=r"argument `settings`: expects `Settings`, but gets a list"),
    ):
        use([name()])  # type: ignore[arg-type]
