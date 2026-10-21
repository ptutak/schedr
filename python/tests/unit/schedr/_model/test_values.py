from collections.abc import Callable as _Callable
from typing import Any

import pytest

from schedr._model.ids import IdError
from schedr._model.values import (
    DagCtxField,
    DagCtxRef,
    ListValue,
    LiteralValue,
    MapValue,
    OutputRef,
    RefValue,
    Template,
    TemplateRef,
    TemplateValue,
)

Validate = _Callable[[str, Any], None]

PLANNED_FOR = DagCtxRef(DagCtxField.PLANNED_FOR)


def test_output_ref_without_a_path_omits_it(validate: Validate) -> None:
    ref = OutputRef("extract")
    assert ref.to_json() == {"kind": "output", "task": "extract"}
    validate("Reference", ref.to_json())


def test_output_ref_with_a_path(validate: Validate) -> None:
    ref = OutputRef("extract", ("rows", 0))
    assert ref.to_json() == {"kind": "output", "task": "extract", "path": ["rows", 0]}
    validate("Reference", ref.to_json())


def test_output_ref_validates_the_task_id() -> None:
    with pytest.raises(IdError):
        OutputRef("bad id")


@pytest.mark.parametrize("step", [-1, True, 1.5])
def test_output_ref_rejects_bad_path_steps(step: object) -> None:
    with pytest.raises(ValueError, match="path"):
        OutputRef("extract", (step,))  # type: ignore[arg-type]


@pytest.mark.parametrize("field", list(DagCtxField))
def test_every_dag_ctx_field_is_in_the_schema(field: DagCtxField, validate: Validate) -> None:
    ref = DagCtxRef(field)
    assert ref.to_json() == {"kind": "dag_ctx", "field": field.value}
    validate("Reference", ref.to_json())


def test_templates_serialize_as_parts(validate: Validate) -> None:
    template = Template(
        ["./notify.sh ", TemplateRef(PLANNED_FOR), " ", TemplateRef(OutputRef("count"), "raw")]
    )
    expected = [
        {"text": "./notify.sh "},
        {"ref": {"kind": "dag_ctx", "field": "planned_for"}},
        {"text": " "},
        {"ref": {"kind": "output", "task": "count", "format": "raw"}},
    ]
    assert template.to_json() == expected
    validate("Template", template.to_json())
    assert template.references() == [PLANNED_FOR, OutputRef("count")]


def test_an_empty_template_has_no_parts() -> None:
    assert Template().to_json() == []
    assert Template().references() == []


def test_values_serialize_externally_tagged(validate: Validate) -> None:
    value = MapValue(
        {
            "names": ListValue([TemplateValue(Template(["a"]))]),
            "day": RefValue(PLANNED_FOR),
            "limit": LiteralValue(10),
        }
    )
    expected = {
        "map": {
            "day": {"ref": {"kind": "dag_ctx", "field": "planned_for"}},
            "limit": {"literal": 10},
            "names": {"list": [{"template": [{"text": "a"}]}]},
        }
    }
    assert value.to_json() == expected
    assert list(value.to_json()["map"]) == ["day", "limit", "names"]
    validate("Value", value.to_json())


def test_literals_keep_tagged_extra_types_as_is(validate: Validate) -> None:
    tagged = {"$datetime": "2026-10-19T03:00:00+00:00"}
    assert LiteralValue(tagged).to_json() == {"literal": tagged}
    validate("Value", LiteralValue(tagged).to_json())


def test_references_are_found_at_any_depth_in_order() -> None:
    value = ListValue(
        [
            LiteralValue("x"),
            RefValue(OutputRef("a")),
            MapValue(
                {
                    "z": RefValue(OutputRef("c")),
                    "k": TemplateValue(Template([TemplateRef(OutputRef("b", (1,)))])),
                }
            ),
            ListValue([RefValue(PLANNED_FOR)]),
        ]
    )
    assert value.references() == [
        OutputRef("a"),
        OutputRef("b", (1,)),
        OutputRef("c"),
        PLANNED_FOR,
    ]
    assert LiteralValue(1).references() == []
