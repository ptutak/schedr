"""Python API for the schedr scheduler.

Define DAGs with ``with Dag(...)``, Python tasks with ``@task``, and other tasks with the
operators; pass outputs between tasks as arguments (see PROJECT.md, Native API).
"""

from importlib.metadata import version

from schedr._secrets import SecretPrefix, secrets
from schedr._templates import text
from schedr.connections import (
    AsyncConnection,
    Connection,
    ConnectionDeniedError,
    ConnectionNotFoundError,
    connection,
)
from schedr.context import SecretError, acleanup_handle, cleanup_handle, dag_ctx, task_ctx
from schedr.dag import Dag, StepDefaults
from schedr.event_triggers import Event, EventTrigger, TriggerFunction, trigger
from schedr.first_of import at_least, first_of
from schedr.groups import Group, group
from schedr.operators import CommandOperator, EmptyOperator, ScriptOperator
from schedr.outcomes import (
    DagHint,
    Data,
    DefaultOutcome,
    Done,
    Outcome,
    RunIf,
    TimedOut,
    TypingHint,
    Waited,
)
from schedr.resources import CommandResource, Resource, ScriptResource, resource
from schedr.sensors import NOT_YET, NotYet, SensorFunction, sensor
from schedr.tasks import Chosen, Mapped, Output, Task, branch, chain, on, task
from schedr.triggers import DAILY, HOURLY, MONTHLY, WEEKLY, YEARLY, Cron, Every, Once, Trigger
from schedr.types import KiB, MiB

__all__ = [
    "DAILY",
    "HOURLY",
    "MONTHLY",
    "NOT_YET",
    "WEEKLY",
    "YEARLY",
    "AsyncConnection",
    "Chosen",
    "CommandOperator",
    "CommandResource",
    "Connection",
    "ConnectionDeniedError",
    "ConnectionNotFoundError",
    "Cron",
    "Dag",
    "DagHint",
    "Data",
    "DefaultOutcome",
    "Done",
    "EmptyOperator",
    "Event",
    "EventTrigger",
    "Every",
    "Group",
    "KiB",
    "Mapped",
    "MiB",
    "NotYet",
    "Once",
    "Outcome",
    "Output",
    "Resource",
    "RunIf",
    "ScriptOperator",
    "ScriptResource",
    "SecretError",
    "SecretPrefix",
    "SensorFunction",
    "StepDefaults",
    "Task",
    "TimedOut",
    "Trigger",
    "TriggerFunction",
    "TypingHint",
    "Waited",
    "acleanup_handle",
    "at_least",
    "branch",
    "chain",
    "cleanup_handle",
    "connection",
    "dag_ctx",
    "first_of",
    "group",
    "on",
    "resource",
    "secrets",
    "sensor",
    "task",
    "task_ctx",
    "text",
    "trigger",
]
__version__ = version("schedr")
