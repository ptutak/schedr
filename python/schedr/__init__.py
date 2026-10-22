"""Python API for the schedr scheduler.

Define DAGs with ``with Dag(...)``, Python tasks with ``@task``, and other tasks with the
operators; pass outputs between tasks as arguments (see PROJECT.md, Native API).
"""

from importlib.metadata import version

from schedr.context import dag_ctx
from schedr.dag import Dag, TaskDefaults
from schedr.operators import CommandOperator, EmptyOperator
from schedr.tasks import Output, chain, task
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
    "EmptyOperator",
    "Every",
    "KiB",
    "MiB",
    "Once",
    "Output",
    "TaskDefaults",
    "Trigger",
    "chain",
    "dag_ctx",
    "task",
]
__version__ = version("schedr")
