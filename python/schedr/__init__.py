"""Python API for the schedr scheduler.

Define DAGs with ``with Dag(...)``, Python tasks with ``@task``, and other tasks with the
operators; pass outputs between tasks as arguments (see PROJECT.md, Native API).
"""

from importlib.metadata import version

from schedr.context import dag_ctx
from schedr.dag import Dag, TaskDefaults
from schedr.groups import Group, group
from schedr.operators import CommandOperator, EmptyOperator
from schedr.outcomes import Data, DefaultOutcome, Done, Outcome, RunIf, TypingHint
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
    "Cron",
    "Dag",
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
    "Task",
    "TaskDefaults",
    "Trigger",
    "TypingHint",
    "branch",
    "chain",
    "dag_ctx",
    "group",
    "on",
    "task",
]
__version__ = version("schedr")
