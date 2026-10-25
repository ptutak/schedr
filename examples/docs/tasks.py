"""A Python task, used twice, plus a shell command, an argument list and an empty task."""

from datetime import timedelta

from schedr import CommandOperator, Dag, EmptyOperator, task

with Dag("tasks_demo", trigger=None):

    @task
    def extract() -> list[str]:
        return ["ada", "grace"]

    @task(timeout=timedelta(minutes=10))
    def load(names: list[str]) -> int:
        print(f"loading {len(names)}")
        return len(names)

    names = extract()
    count = load(names)
    again = load.override(task_id="load_again")(names)

    CommandOperator(
        "announce",
        command=t"echo loaded {count} in $REGION",
        env={"REGION": "eu"},
    )
    CommandOperator("list_one", command=["echo", names[0]])
    done = EmptyOperator("done")
    [count, again] >> done
