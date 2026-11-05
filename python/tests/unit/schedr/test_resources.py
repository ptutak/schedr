"""Resources in the DAG: two parts, always (decisions 223, 224, 226, 259-265)."""

import warnings
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from schedr import (
    CommandOperator,
    CommandResource,
    Dag,
    DagHint,
    EmptyOperator,
    Resource,
    Task,
    branch,
    group,
    resource,
    task,
)
from schedr.resources import ResourceFunction


@dataclass
class Cluster:
    cluster_id: str


@dataclass
class Rows:
    n: int


@dataclass
class Gone:
    why: str


def as_json(dag: Dag) -> dict[str, Any]:
    assert dag.model is not None
    return dag.model.to_json()


def edges(dag: Dag) -> set[tuple[str, str, str]]:
    return {(e["from"], e["to"], e["kind"]) for e in as_json(dag)["edges"]}


def a_task(dag: Dag, task_id: str) -> dict[str, Any]:
    return next(t for t in as_json(dag)["tasks"] if t["id"] == task_id)


def make_cluster() -> ResourceFunction[[int], Cluster]:
    @resource
    def cluster(size: int) -> Cluster:
        return Cluster(str(size))

    return cluster


@task
def load(c: Cluster) -> int:
    return len(c.cluster_id)


@task
def warm() -> None:
    pass


def test_a_resource_has_its_two_parts_and_its_users() -> None:
    cluster = make_cluster()

    @cluster.cleanup
    def delete(c: Cluster) -> None:
        del c

    with Dag("two_parts") as dag:
        made = cluster(8)
        load(made)
        warm.override(uses=[cluster])()
    assert as_json(dag)["resources"] == [
        {"setup": "cluster", "cleanup": "cluster.cleanup", "users": ["load", "warm"]}
    ]
    assert edges(dag) == {
        ("cluster", "load", "data"),
        ("cluster", "warm", "requirement"),
        ("cluster", "cluster.cleanup", "data"),
    }
    assert a_task(dag, "cluster.cleanup")["settings"]["retries"] == 3


def test_a_resource_without_cleanup_still_has_one_empty() -> None:
    cluster = make_cluster()
    with Dag("empty_cleanup") as dag:
        load(cluster(8))
    assert a_task(dag, "cluster.cleanup")["kind"] == "empty"
    assert ("cluster", "cluster.cleanup", "requirement") in edges(dag)


def test_a_cleanup_may_come_after_the_call_and_users_before_the_setup() -> None:
    cluster = make_cluster()
    with Dag("any_order") as dag:
        warm.override(uses=[cluster])()
        cluster(8)

        @cluster.cleanup
        def delete(c: Cluster) -> None:
            del c

    assert ("cluster", "warm", "requirement") in edges(dag)
    assert a_task(dag, "cluster.cleanup")["kind"] != "empty"


def test_uses_takes_the_setups_output_and_works_on_operators_and_groups() -> None:
    cluster = make_cluster()
    with Dag("uses_forms") as dag:
        made = cluster(8)
        EmptyOperator("mark", uses=[made])
        CommandOperator(task_id="run", command="true", uses=[cluster])
        with group("g", uses=[cluster]):
            warm()
    assert {
        ("cluster", "mark", "requirement"),
        ("cluster", "run", "requirement"),
        ("cluster", "g.warm", "requirement"),
    } <= edges(dag)


def test_a_cleanup_is_in_its_setups_group_and_what_follows_the_group_waits_for_it() -> None:
    cluster = make_cluster()
    with Dag("in_a_group") as dag:
        with group("g") as inside:
            load(cluster(8))
        inside >> EmptyOperator("after")
    assert a_task(dag, "g.cluster.cleanup")["group"] == "g"
    assert {("g.load", "after", "order"), ("g.cluster.cleanup", "after", "order")} <= edges(dag)


def test_a_cleanup_not_blocking_holds_nothing_back() -> None:
    cluster = make_cluster()

    @cluster.cleanup(blocking=False)
    def delete(c: Cluster) -> None:
        del c

    with Dag("not_blocking") as dag:
        with group("g") as inside:
            load(cluster(8))
        inside >> EmptyOperator("after")
    assert ("g.load", "after", "order") in edges(dag)
    assert not any(start == "g.cluster.cleanup" for start, _, _ in edges(dag))


def test_a_task_after_the_group_using_the_resource_comes_before_its_cleanup() -> None:
    cluster = make_cluster()
    with Dag("still_used") as dag:
        with group("g") as inside:
            made = cluster(8)
            warm.override(uses=[made])()
        inside >> load(made)
    assert ("g.warm", "load", "order") in edges(dag)
    assert not any(start == "g.cluster.cleanup" for start, _, _ in edges(dag))
    assert set(as_json(dag)["resources"][0]["users"]) == {"g.warm", "load"}


def test_a_task_before_a_user_doesnt_wait_for_the_cleanup_either() -> None:
    cluster = make_cluster()
    with Dag("before_a_user") as dag:
        with group("g") as inside:
            made = cluster(8)
            warm.override(uses=[made])()
        between = EmptyOperator("between")
        inside >> between
        between >> load(made)
    assert not any(start == "g.cluster.cleanup" for start, _, _ in edges(dag))


def test_two_groups_using_each_others_resources_dont_wait_in_a_circle() -> None:
    one = make_cluster()

    @resource
    def other() -> Cluster:
        return Cluster("o")

    with Dag("crossed") as dag:
        with group("a") as first:
            load.override(task_id="uses_one")(one(8))
        with group("b") as second:
            load.override(task_id="uses_other")(other())
        tail_a = EmptyOperator("tail_a", uses=[other])
        tail_b = EmptyOperator("tail_b", uses=[one])
        first >> tail_a
        second >> tail_b
    # Both waits would close a circle through the tails, so the second is left out.
    assert ("a.cluster.cleanup", "tail_a", "order") in edges(dag)
    assert ("b.other.cleanup", "tail_b", "order") not in edges(dag)


def test_a_resource_outside_groups_holds_nothing_back() -> None:
    cluster = make_cluster()
    with Dag("no_group") as dag:
        used = load(cluster(8))
        used >> EmptyOperator("after")
    assert not any(start == "cluster.cleanup" for start, _, _ in edges(dag))


def test_a_cleanup_inherits_the_setups_secrets_connections_and_queue_and_adds_its_own() -> None:
    @resource(queue="big", secrets=["a"], connections=["db"])
    def cluster() -> Cluster:
        return Cluster("c")

    @cluster.cleanup(secrets=["b"], retries=1, retry_delay=timedelta(seconds=2))
    def delete(c: Cluster) -> None:
        del c

    with Dag("inherits") as dag:
        load(cluster())
    found = a_task(dag, "cluster.cleanup")
    assert found["settings"]["queue"] == "big"
    assert found["settings"]["retries"] == 1
    assert found["settings"]["retry_delay_ms"] == 2000
    assert len(found["secrets"]) == 2
    assert found["connections"] == ["db"]


def test_a_resource_with_no_value_has_a_cleanup_with_no_parameter() -> None:
    @resource
    def lock() -> None:
        pass

    @lock.cleanup
    def release() -> None:
        pass

    with Dag("no_value") as dag:
        lock()
        warm.override(uses=[lock])()
    assert ("lock", "lock.cleanup", "requirement") in edges(dag)


def test_a_resource_without_setup_code_is_finished_by_the_master() -> None:
    with Dag("static") as dag:
        tmp = Resource("tmp-prefix")

        @tmp.cleanup
        def drop() -> None:
            pass

        warm.override(uses=[tmp])()
    assert a_task(dag, "tmp-prefix")["kind"] == "empty"
    assert as_json(dag)["resources"][0]["users"] == ["warm"]


def test_a_setup_keeps_its_own_per_task_cleanup_for_failing_halfway() -> None:
    def undo(handles: list[Cluster]) -> None:
        del handles

    @resource(cleanup=undo)
    def cluster() -> Cluster:
        return Cluster("c")

    with Dag("halfway") as dag:
        load(cluster())
    assert "cleanup" in a_task(dag, "cluster")


def test_an_override_is_the_same_resource() -> None:
    cluster = make_cluster()
    with Dag("overridden") as dag:
        load(cluster.override(retries=5)(8))
        warm.override(uses=[cluster])()
    assert as_json(dag)["resources"][0]["users"] == ["load", "warm"]


def test_a_resource_with_no_users_gets_a_hint() -> None:
    cluster = make_cluster()
    with pytest.warns(DagHint, match="no users"), Dag("unused"):
        cluster(8)


def test_a_resource_with_users_gets_no_hint() -> None:
    cluster = make_cluster()
    with warnings.catch_warnings():
        warnings.simplefilter("error", DagHint)
        with Dag("used"):
            load(cluster(8))


def test_a_setup_called_twice_is_refused() -> None:
    cluster = make_cluster()
    with Dag("twice"):
        cluster(1)
        again = cluster.override(task_id="again")
        with pytest.raises(TypeError, match="once in a DAG"):
            again(2)


def test_a_mapped_setup_is_refused() -> None:
    cluster = make_cluster()
    with pytest.raises(TypeError, match="can't be mapped"), Dag("mapped"):
        cluster.expand(size=[1, 2])


def test_an_outcome_union_from_a_setup_is_refused() -> None:
    @resource
    def cluster() -> Rows | Gone:
        return Rows(1)

    with pytest.raises(TypeError, match="outcome union"), Dag("union"):
        cluster()


def test_a_second_cleanup_is_refused() -> None:
    cluster = make_cluster()

    @cluster.cleanup
    def one(c: Cluster) -> None:
        del c

    with pytest.raises(TypeError, match="has a cleanup already"):

        @cluster.cleanup
        def two(c: Cluster) -> None:
            del c


def test_the_same_cleanup_again_is_its_file_imported_again() -> None:
    cluster = make_cluster()

    def imported() -> None:
        @cluster.cleanup
        def delete(c: Cluster) -> None:
            del c

    imported()
    imported()
    with Dag("imported_twice") as dag:
        load(cluster(8))
    assert a_task(dag, "cluster.cleanup")["kind"] != "empty"


def test_a_setup_returning_an_alias_of_none_has_no_value() -> None:
    type NoValue = None

    @resource
    def lock() -> NoValue:
        pass

    @lock.cleanup
    def release() -> None:
        pass

    with Dag("alias") as dag:
        lock()
        warm.override(uses=[lock])()
    assert ("lock", "lock.cleanup", "requirement") in edges(dag)


@branch
def choose() -> list[Task]:
    return []


@task
def number() -> int:
    return 1


@task
def takes(n: int) -> None:
    del n


def test_a_requirement_doesnt_make_an_input_optional() -> None:
    cluster = make_cluster()
    with Dag("still_required") as dag:
        cluster(8)
        made = number()
        choose() >> made
        takes.override(uses=[cluster])(made)
    assert ("cluster", "takes", "requirement") in edges(dag)


def test_a_requirement_guards_the_setups_own_value() -> None:
    @resource
    def setup() -> int:
        return 1

    with Dag("guarded") as dag:
        value = setup()
        choose() >> value
        used = takes.override(uses=[setup])(value)
        EmptyOperator("independent") >> used
    assert ("setup", "takes", "requirement") in edges(dag)


def test_a_requirement_is_no_other_way_to_run() -> None:
    cluster = make_cluster()

    def define() -> None:
        with Dag("skipped_input"):
            cluster(8)
            made = number.override(uses=[cluster])()
            choose() >> made
            EmptyOperator("other") >> takes(made)

    with pytest.raises(TypeError, match="is None"):
        define()


def test_an_overridden_setup_has_the_same_cleanup() -> None:
    cluster = make_cluster()
    renamed = cluster.override(task_id="renamed")

    @renamed.cleanup(retries=1)
    def delete(c: Cluster) -> None:
        del c

    with Dag("through_override") as dag:
        load(cluster(8))
    assert a_task(dag, "cluster.cleanup")["settings"]["retries"] == 1


def test_a_cleanup_must_take_what_the_resource_gives() -> None:
    cluster = make_cluster()

    @cluster.cleanup
    def nothing() -> None:
        pass

    with pytest.raises(TypeError, match="resource's value"), Dag("too_few"):
        load(cluster(8))


def test_a_cleanup_taking_another_type_is_refused() -> None:
    cluster = make_cluster()

    @cluster.cleanup
    def wrong(c: Rows) -> None:
        del c

    with pytest.raises(TypeError), Dag("wrong_type"):
        load(cluster(8))


def test_uses_needs_a_resource_of_this_dag() -> None:
    cluster = make_cluster()
    with pytest.raises(TypeError, match="isn't set up in this DAG"), Dag("not_here"):
        warm.override(uses=[cluster])()
    with pytest.raises(TypeError, match="takes resources"), Dag("not_a_resource"):
        warm.override(uses=["cluster"])()


def test_the_cleanup_cant_clash_with_a_task() -> None:
    cluster = make_cluster()
    with Dag("clash"):
        with group("cluster"):
            EmptyOperator("cleanup")
        with pytest.raises(ValueError, match="group's ID"):
            cluster(8)


def test_a_cleanup_carries_blocking_and_its_unschedulable_timeout() -> None:
    cluster = make_cluster()

    @cluster.cleanup(blocking=False, unschedulable_timeout=timedelta(hours=1))
    def delete(c: Cluster) -> None:
        del c

    with Dag("settings") as dag:
        load(cluster(8))
    assert as_json(dag)["resources"] == [
        {
            "setup": "cluster",
            "cleanup": "cluster.cleanup",
            "users": ["load"],
            "blocking": False,
            "unschedulable_timeout_ms": 3_600_000,
        }
    ]


def test_a_negative_unschedulable_timeout_is_refused() -> None:
    cluster = make_cluster()

    @cluster.cleanup(unschedulable_timeout=timedelta(seconds=-1))
    def delete(c: Cluster) -> None:
        del c

    with pytest.raises(ValueError, match="unschedulable_timeout"), Dag("negative"):
        load(cluster(8))


def ref(task_id: str, *path: str) -> dict[str, Any]:
    reference: dict[str, Any] = {"kind": "output", "task": task_id}
    if path:
        reference["path"] = list(path)
    return {"ref": reference}


def test_a_command_resource_reports_its_value_and_its_users_take_it() -> None:
    with Dag("command_setup") as dag:
        cluster = CommandResource("cluster", ["./create.sh", "8"], value=Cluster, retries=2)
        cluster.cleanup_command(
            ["./delete.sh", t"{cluster.cluster_id}"], env={"C": cluster.cluster_id}
        )
        load(cluster.value)
        warm.override(uses=[cluster])()
    setup = a_task(dag, "cluster")
    assert (setup["kind"], setup["resource"], setup["settings"]["retries"]) == (
        "command",
        {"role": "setup"},
        2,
    )
    assert setup["output_type"]["name"] == "Cluster"
    cleanup = a_task(dag, "cluster.cleanup")
    assert cleanup["resource"] == {"role": "cleanup", "setup": "cluster"}
    assert cleanup["command"]["argv"][1] == [ref("cluster", "cluster_id")]
    assert cleanup["env"] == {"C": [ref("cluster", "cluster_id")]}
    assert cleanup["settings"]["retries"] == 3
    assert edges(dag) >= {
        ("cluster", "load", "data"),
        ("cluster", "warm", "requirement"),
        ("cluster", "cluster.cleanup", "data"),
    }
    assert as_json(dag)["resources"][0]["users"] == ["load", "warm"]


def test_a_command_resource_without_a_value_has_none() -> None:
    with Dag("valueless") as dag:
        lock = CommandResource("lock", "./lock.sh")
        lock.cleanup_command("./unlock.sh")
        warm.override(uses=[lock])()
    assert a_task(dag, "lock")["output_type"] == {"type": "none"}
    assert ("lock", "lock.cleanup", "requirement") in edges(dag)

    @resource
    def held() -> None:
        pass

    held.cleanup_command(["./unlock.sh", t"{held.value}"])

    def define() -> None:
        with Dag("no_value"):
            warm.override(uses=[held])()
            held()

    with pytest.raises(TypeError, match="has no value"):
        define()


def test_a_python_resource_can_be_cleaned_by_a_command() -> None:
    cluster = make_cluster()
    cluster.cleanup_command(["./delete.sh", t"{cluster.value.cluster_id}"], queue="ops")
    with Dag("python_setup") as dag:
        load(cluster(8))
    cleanup = a_task(dag, "cluster.cleanup")
    assert (cleanup["kind"], cleanup["settings"]["queue"]) == ("command", "ops")
    assert cleanup["command"]["argv"][1] == [ref("cluster", "cluster_id")]

    tmp_dag = Dag("tmp")
    with tmp_dag:
        tmp = Resource("tmp")
        tmp.cleanup_command("rm -rf /tmp/stage")
        warm.override(uses=[tmp])()
    assert a_task(tmp_dag, "tmp.cleanup")["resource"] == {"role": "cleanup", "setup": "tmp"}


def test_a_resource_has_one_cleanup_function_or_command() -> None:
    cluster = make_cluster()

    @cluster.cleanup
    def delete(c: Cluster) -> None:
        del c

    with pytest.raises(TypeError, match="has a cleanup already"):
        cluster.cleanup_command("./delete.sh")

    other = make_cluster()

    def imported() -> None:
        other.cleanup_command("./delete.sh")

    imported()
    imported()  # The same call again is its file imported again.
    with pytest.raises(TypeError, match="has a cleanup already"):
        other.cleanup_command("./delete.sh")


@pytest.mark.parametrize("command", [[], 5])
def test_a_cleanup_command_of_the_wrong_type_is_refused(command: Any) -> None:
    with pytest.raises(TypeError, match="cleanup_command"):
        make_cluster().cleanup_command(command)


def test_a_command_resource_is_set_up_once_and_not_outside_a_dag() -> None:
    with pytest.raises(RuntimeError):
        CommandResource("cluster", "./c.sh")
    with pytest.raises(TypeError, match="value="), Dag("bad_value"):
        CommandResource("cluster", "./c.sh", value=object)
