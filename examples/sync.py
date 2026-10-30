"""The example DAG of PROJECT.md (Native API): extract, load, notify; daily at 03:00."""

from datetime import timedelta

from schedr import CommandOperator, Cron, Dag, TaskDefaults, dag_ctx, task

with Dag(
    "sync_customers",
    trigger=Cron("0 3 * * *", tz="Europe/Warsaw", start="2026-01-01"),
    defaults=TaskDefaults(retries=2, retry_delay=timedelta(minutes=5)),
):

    @task
    def extract() -> list[dict[str, str]]:
        return [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Grace"}]

    @task(timeout=timedelta(minutes=10))
    def load(rows: list[dict[str, str]]) -> int:
        print(f"loading {len(rows)} customers")
        return len(rows)

    rows = extract()  # Output[list[dict[str, str]]]
    count = load(rows)  # load depends on extract (data flow)

    CommandOperator(  # depends on load: its command uses `count`
        "notify",
        command=["echo", t"synced {count} customers for {dag_ctx.run.planned_for}"],
    )
