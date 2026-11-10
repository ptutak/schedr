"""Python API for the schedr scheduler.

Define DAGs with ``with Dag(...)``, Python tasks with ``@task``, and other tasks with the
operators; pass outputs between tasks as arguments (see archive/PROJECT.md, Native API).
"""

from importlib.metadata import version

from schedr._secrets import SecretPrefix, secrets
from schedr._templates import text
from schedr.chained import Chained, TriggerDagOperator
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
from schedr.named_events import EventNamed
from schedr.notify import Notify, WebhookChannel
from schedr.operators import CommandOperator, EmptyOperator, ScriptOperator
from schedr.outcomes import (
    DagHint,
    Data,
    DefaultOutcome,
    Done,
    Failed,
    Outcome,
    RunIf,
    TimedOut,
    TypingHint,
    Waited,
)
from schedr.resources import CommandResource, Resource, ScriptResource, resource
from schedr.run_of import RunOf, RunResult, wait_for
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
    "Chained",
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
    "EventNamed",
    "EventTrigger",
    "Every",
    "Failed",
    "Group",
    "KiB",
    "Mapped",
    "MiB",
    "NotYet",
    "Notify",
    "Once",
    "Outcome",
    "Output",
    "Resource",
    "RunIf",
    "RunOf",
    "RunResult",
    "ScriptOperator",
    "ScriptResource",
    "SecretError",
    "SecretPrefix",
    "SensorFunction",
    "StepDefaults",
    "Task",
    "TimedOut",
    "Trigger",
    "TriggerDagOperator",
    "TriggerFunction",
    "TypingHint",
    "Waited",
    "WebhookChannel",
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
    "wait_for",
]
__version__ = version("schedr")
