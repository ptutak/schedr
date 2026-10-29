from dataclasses import dataclass
from typing import Any

import pydantic
import pytest
from pydantic import BaseModel

from schedr._serialization import TypeModelError
from schedr._serialization.models import (
    check_version,
    dump,
    is_model,
    is_model_value,
    schema,
    validate,
)


class Point(BaseModel):
    x: int
    y: int = 0


@dataclass
class Plain:
    x: int


def test_models_are_recognised_by_their_base_class() -> None:
    assert is_model(Point)
    assert not is_model(Plain)
    assert not is_model(int)
    assert not is_model("Point")
    assert is_model_value(Point(x=1))
    assert not is_model_value(Plain(1))


def test_a_model_is_stored_by_attribute_names_and_read_back() -> None:
    class Aliased(BaseModel):
        value: int = pydantic.Field(alias="Value")

    stored = dump(Aliased(Value=3))
    assert stored == {"value": 3}
    assert validate(Aliased, stored) == Aliased(Value=3)
    with pytest.raises(ValueError, match="validation error"):
        validate(Point, {"x": "not a number"})


def test_the_schema_on_each_side() -> None:
    assert schema(Point, "output")["required"] == ["x", "y"]
    assert schema(Point, "input")["required"] == ["x"]


class Handle:
    """Neither a model nor a dataclass: Pydantic has no JSON Schema for it."""


def test_a_model_pydantic_cant_describe_is_refused() -> None:
    class Odd(BaseModel):
        model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
        thing: Handle

    with pytest.raises(TypeModelError, match=r"`Odd`: Pydantic can't describe it as JSON Schema"):
        schema(Odd, "output")


@pytest.mark.parametrize("version", ["2.10.6", "3.0.0", "1.10.0"])
def test_an_unsupported_pydantic_is_refused(monkeypatch: Any, version: str) -> None:
    monkeypatch.setattr(pydantic, "VERSION", version)
    with pytest.raises(
        TypeModelError,
        match=rf"Pydantic {version} isn't supported for outputs: install `schedr\[pydantic\]`",
    ):
        check_version()


def test_the_supported_pydantic_passes() -> None:
    check_version()
