import pytest

from schedr._model import DagCtxField, DagCtxRef
from schedr.context import DagCtxReference, dag_ctx


@pytest.mark.parametrize(
    ("reference", "field"),
    [
        (dag_ctx.run.id, DagCtxField.RUN_ID),
        (dag_ctx.run.kind, DagCtxField.RUN_KIND),
        (dag_ctx.run.planned_for, DagCtxField.PLANNED_FOR),
        (dag_ctx.run.since, DagCtxField.SINCE),
        (dag_ctx.dag_id, DagCtxField.DAG_ID),
    ],
)
def test_fields_are_references(reference: DagCtxReference, field: DagCtxField) -> None:
    assert reference == DagCtxReference(field)
    assert reference._schedr_reference() == DagCtxRef(field)


def test_a_reference_cant_be_used_as_a_value() -> None:
    with pytest.raises(TypeError, match="can't be used as a value in the DAG file"):
        bool(dag_ctx.run.planned_for)
    with pytest.raises(TypeError, match="can't be used as a value"):
        str(dag_ctx.run.since)


def test_params_come_later() -> None:
    with pytest.raises(NotImplementedError, match="milestone M4"):
        _ = dag_ctx.params
