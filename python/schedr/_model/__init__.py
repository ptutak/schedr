"""Private builder API: the DAG JSON model as Python types.

These types mirror the Rust crate ``schedr-model``, which is the reference for every shape
(see PROJECT.md, Rust/Python integration: shared model and schemas). Each type's
``to_json()`` produces exactly the JSON the Rust types accept; tests validate it against the
committed ``schema/dag.schema.json``.

The native API and the compatibility layers build DAGs through this package. It isn't
user API: names and signatures may change between versions.
"""

from schedr._model.dag import Dag, DagLocation, Edge, EdgeKind
from schedr._model.ids import IdError, validate_dag_id, validate_task_id
from schedr._model.tasks import (
    ArgvCommand,
    CommandKind,
    CommandLine,
    EmptyKind,
    PythonKind,
    ShellCommand,
    Task,
    TaskKind,
    TaskSettings,
)
from schedr._model.timestamps import format_timestamp, parse_moment
from schedr._model.trigger import (
    CronTrigger,
    EveryTrigger,
    NoTrigger,
    OnceTrigger,
    Trigger,
)
from schedr._model.types import (
    DataclassType,
    DictType,
    Field,
    ListType,
    LiteralType,
    ScalarType,
    TupleType,
    TypedDictType,
    TypeDesc,
    UnionType,
)
from schedr._model.values import (
    DagCtxField,
    DagCtxRef,
    ListValue,
    LiteralValue,
    MapValue,
    OutcomeRef,
    OutputRef,
    Reference,
    RefValue,
    Template,
    TemplatePart,
    TemplateRef,
    TemplateValue,
    Value,
)
from schedr._model.version import SchedrVersion

__all__ = [
    "ArgvCommand",
    "CommandKind",
    "CommandLine",
    "CronTrigger",
    "Dag",
    "DagCtxField",
    "DagCtxRef",
    "DagLocation",
    "DataclassType",
    "DictType",
    "Edge",
    "EdgeKind",
    "EmptyKind",
    "EveryTrigger",
    "Field",
    "IdError",
    "ListType",
    "ListValue",
    "LiteralType",
    "LiteralValue",
    "MapValue",
    "NoTrigger",
    "OnceTrigger",
    "OutcomeRef",
    "OutputRef",
    "PythonKind",
    "RefValue",
    "Reference",
    "ScalarType",
    "SchedrVersion",
    "ShellCommand",
    "Task",
    "TaskKind",
    "TaskSettings",
    "Template",
    "TemplatePart",
    "TemplateRef",
    "TemplateValue",
    "Trigger",
    "TupleType",
    "TypeDesc",
    "TypedDictType",
    "UnionType",
    "Value",
    "format_timestamp",
    "parse_moment",
    "validate_dag_id",
    "validate_task_id",
]
