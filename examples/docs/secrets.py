"""Secrets: read inside a Python task, under a prefix, and given to a command in `env`."""

from typing import cast

from schedr import CommandOperator, Dag, SecretError, SecretPrefix, secrets, task, task_ctx


@task(secrets=["etl/db#password"])
def connect() -> str:
    # Fetched before the task started; masked in its log and output.
    password = cast("str", task_ctx.secret("etl/db#password"))
    url = f"postgresql://etl:{password}@db.example.com/etl"
    print(f"connecting to {url}")
    return url


@task(secrets=["etl/api#token", SecretPrefix("tenants/")])
def call_api(tenant: str) -> str:
    token = cast("str", task_ctx.secret("etl/api#token"))
    try:
        # Under the prefix: chosen at run time, fetched now.
        key = cast("str", task_ctx.secret(f"tenants/{tenant}#api_key"))
    except SecretError as error:
        if error.kind != "secret_not_found":
            raise
        return f"{tenant}: no key of its own"
    return f"{tenant}: a key of {len(key)} characters, a token of {len(token)}"


with Dag("secrets_demo"):
    connect()
    call_api("acme")
    CommandOperator(
        "dump",
        ["sh", "-c", 'test -n "$PGPASSWORD" && echo "the password is set"'],
        env={"PGPASSWORD": secrets["etl/db#password"]},
    )
