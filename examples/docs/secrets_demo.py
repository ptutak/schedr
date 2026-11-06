"""Secrets: passed to a task, read inside one, under a prefix, and in a command's `env`."""

from schedr import CommandOperator, Dag, SecretError, SecretPrefix, secrets, task, task_ctx, text


@task
def connect(url: str) -> str:
    # The agent filled in the password before the task started.
    print(f"connecting to {url}")
    return url


@task(secrets=["etl/api#token", SecretPrefix("tenants/")])
def call_api(tenant: str) -> str:
    token = task_ctx.secret("etl/api", "token")
    try:
        # Under the prefix: chosen at run time, fetched now.
        key = task_ctx.secret(f"tenants/{tenant}", "api_key")
    except SecretError as error:
        if error.kind != "secret_not_found":
            raise
        return f"{tenant}: no key of its own"
    return f"{tenant}: a key of {len(key)} characters, a token of {len(token)}"


with Dag("secrets_demo"):
    connect(text(t"postgresql://etl:{secrets['etl/db']['password']}@db.example.com/etl"))
    call_api("acme")
    CommandOperator(
        "dump",
        ["sh", "-c", 'test -n "$PGPASSWORD" && echo "the password is set"'],
        env={
            "PGPASSWORD": secrets["etl/db"]["password"],
            "PGPORT": secrets["etl/db"].get("port", "5432"),
        },
    )
