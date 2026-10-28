"""Python API for the schedr scheduler.

Define DAGs with ``with Dag(...)``, Python tasks with ``@task``, and other tasks with the
operators; pass outputs between tasks as arguments (see PROJECT.md, Native API).
"""

from importlib.metadata import version

from schedr.context import dag_ctx
from schedr.dag import Dag, TaskDefaults
from schedr.operators import CommandOperator, EmptyOperator
from schedr.outcomes import DefaultOutcome, Outcome, RunIf
from schedr.tasks import Output, Task, branch, chain, task
from schedr.triggers import DAILY, HOURLY, MONTHLY, WEEKLY, YEARLY, Cron, Every, Once, Trigger
from schedr.types import KiB, MiB

__all__ = [
    "DAILY",
    "HOURLY",
    "MONTHLY",
    "WEEKLY",
    "YEARLY",
    "CommandOperator",
    "Cron",
    "Dag",
    "DefaultOutcome",
    "EmptyOperator",
    "Every",
    "KiB",
    "MiB",
    "Once",
    "Outcome",
    "Output",
    "RunIf",
    "Task",
    "TaskDefaults",
    "Trigger",
    "branch",
    "chain",
    "dag_ctx",
    "task",
]
__version__ = version("schedr")
