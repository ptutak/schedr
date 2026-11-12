"""Cleaning up after a task, however it ended."""

import shutil
import tempfile
from dataclasses import dataclass
from datetime import timedelta

from schedr import CommandOperator, Dag, StepDefaults, cleanup_handle, task


@dataclass
class TempDir:
    """A directory the attempt made."""

    path: str


@dataclass
class ExportJob:
    """A job the attempt started on the warehouse."""

    job_id: str


def cancel_if_running(job_id: str) -> None:
    """Stands in for the warehouse's client."""
    print(f"cancelling {job_id}")


def undo_export(day: str, handles: list[ExportJob | TempDir]) -> None:
    """Runs after each attempt: it must be fine to run twice, and after a success."""
    print(f"cleaning up the export of {day}")
    for handle in handles:
        match handle:
            case ExportJob(job_id=job_id):
                cancel_if_running(job_id)
            case TempDir(path=path):
                shutil.rmtree(path, ignore_errors=True)


@task(cleanup=undo_export, cleanup_timeout=timedelta(seconds=20))
def export(day: str) -> None:
    """Registers each thing before relying on it."""
    cleanup_handle(TempDir(tempfile.mkdtemp()))
    cleanup_handle(ExportJob(f"job-{day}"))


with Dag(
    "cleanup",
    description="Shows a task's cleanup running however the task ended, retried once if it fails.",
    defaults=StepDefaults(cleanup_retries=1),
):
    export("2026-11-04") >> CommandOperator(
        "publish",
        'id=$(date +%s); schedr task-ctx cleanup add Publication id="$id"',
        cleanup="schedr task-ctx cleanup list Publication --field id | xargs -r echo withdrawing",
        cleanup_handles=["Publication"],
        cleanup_timeout=timedelta(seconds=20),
    )
