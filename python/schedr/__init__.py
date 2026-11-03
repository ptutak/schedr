"""Python API for the schedr scheduler.

Define DAGs with ``with Dag(...)``, Python tasks with ``@task``, and other tasks with the
operators; pass outputs between tasks as arguments (see PROJECT.md, Native API).
"""

from importlib.metadata import version

from schedr._secrets import SecretPrefix, secrets
from schedr._templates import text
from schedr.connections import (
    Connection,
    ConnectionDeniedError,
    ConnectionNotFoundError,
    connection,
)
from schedr.context import SecretError, dag_ctx, task_ctx
from schedr.dag import Dag, TaskDefaults
from schedr.groups import Group, group
from schedr.operators import CommandOperator, EmptyOperator
from schedr.outcomes import DagHint, Data, DefaultOutcome, Done, Outcome, RunIf, TypingHint
from schedr.tasks import Chosen, Mapped, Output, Task, branch, chain, on, task
from schedr.triggers import DAILY, HOURLY, MONTHLY, WEEKLY, YEARLY, Cron, Every, Once, Trigger
from schedr.types import KiB, MiB

__all__ = [
    "DAILY",
    "HOURLY",
    "MONTHLY",
    "WEEKLY",
    "YEARLY",
    "Chosen",
    "CommandOperator",
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
    "Every",
    "Group",
    "KiB",
    "Mapped",
    "MiB",
    "Once",
    "Outcome",
    "Output",
    "RunIf",
    "SecretError",
    "SecretPrefix",
    "Task",
    "TaskDefaults",
    "Trigger",
    "TypingHint",
    "branch",
    "chain",
    "connection",
    "dag_ctx",
    "group",
    "on",
    "secrets",
    "task",
    "task_ctx",
    "text",
]
__version__ = version("schedr")
