"""Run a small command per element in mapped Python tasks."""

import subprocess

from schedr import Dag, task


@task
def run_command(name: str) -> str:
    completed = subprocess.run(
        ["printf", "hello %s", name], check=True, capture_output=True, text=True
    )
    return completed.stdout


with Dag("mapped_process", description="Runs a small command for each element of a list."):
    run_command.expand(name=["eu", "us"])
