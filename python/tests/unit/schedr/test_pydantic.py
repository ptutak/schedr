import warnings

import pytest
from pydantic import BaseModel

from schedr import TypingHint
from schedr.outcomes import from_return_type
from schedr.pydantic import Model


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
    # Routing on them needs Pydantic outputs (the type model's descriptor; B36).
    with warnings.catch_warnings():
        warnings.simplefilter("error", TypingHint)
        assert from_return_type(Remote | Missing, "task `locate`").names == ("Remote", "Missing")


def test_a_plain_base_model_outcome_gets_a_hint_naming_model() -> None:
    class PlainRemote(BaseModel):
        url: str

    with pytest.warns(TypingHint, match=r"derive it from schedr\.pydantic\.Model"):
        from_return_type(PlainRemote | Missing, "task `locate`")
