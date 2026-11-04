"""A DAG declaring connections, for the Connections browser tests (M4.7e): `plain` names no
secret (its check and test pass), `settings` names one nobody set (its check fails)."""

from schedr import Dag, secrets, task
from schedr.connections import Generic

plain = Generic("plain", region="eu")
settings = Generic("settings", region="eu", token=secrets["ui/settings"]["token"])


@task(connections=[plain, settings])
def uses_them() -> None:
    """Never run by the tests."""


with Dag("with_connections"):
    uses_them()
