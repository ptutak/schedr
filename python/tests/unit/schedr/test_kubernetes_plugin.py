"""The Kubernetes example extension (M5.2i2, M5.2i11): its plugin calling its service, the service
against a fake cluster held in memory: what it creates, watches, streams, returns and deletes.
The real cluster on kind: `scripts/kind-extension.sh` and `make demo-kind`."""

import asyncio
import contextlib
import importlib.machinery
import importlib.util
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from schedr._service import ServiceAttempt, _frame, _read_frame
from schedr.runner.protocol import Channel

PLUGIN = (
    Path(__file__).parents[4] / "examples" / "extensions" / "kubernetes" / "schedr-ext-kubernetes"
)


def load() -> ModuleType:
    """The plugin as a module (its file has no `.py`)."""
    loader = importlib.machinery.SourceFileLoader("schedr_ext_kubernetes", str(PLUGIN))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[loader.name] = module
    loader.exec_module(module)
    return module


def pod(phase: str, state: dict[str, Any] | None = None) -> dict[str, Any]:
    """A pod's state as the API sends it."""
    statuses = [{"state": state}] if state is not None else []
    return {"status": {"phase": phase, "containerStatuses": statuses}}


class FakeCluster:
    """A namespace held in memory: each Job created plays its scenario."""

    def __init__(self, scenario: Mapping[str, Any], namespace: str = "batch") -> None:
        self.namespace = namespace
        self.scenario = dict(scenario)
        self.calls: list[tuple[str, Any]] = []
        self.queues: dict[str, asyncio.Queue[tuple[str, dict[str, Any]]]] = {}
        self.closed = False

    async def open(self) -> None:
        self.calls.append(("open", None))

    async def close(self) -> None:
        self.closed = True

    async def create_secret(self, name: str, values: Mapping[str, str]) -> None:
        self.calls.append(("create_secret", (name, dict(values))))

    async def create_job(self, manifest: Mapping[str, Any]) -> str:
        self.calls.append(("create_job", manifest))
        if self.scenario.get("create_fails"):
            raise RuntimeError("422: the Job is invalid")
        job = manifest["metadata"]["name"]
        pod_meta = {"name": f"{job}-pod", "labels": {"job-name": job}}
        status = self.scenario.get("status")
        job_event = ("MODIFIED", {"metadata": {"name": job}, "status": status})
        # The Jobs' watch may come before the pods' (two streams).
        if status is not None and self.scenario.get("status_first"):
            await self.queue("jobs").put(job_event)
        for i, state in enumerate(self.scenario.get("pods", [])):
            kind = "ADDED" if i == 0 else "MODIFIED"
            await self.queue("pods").put((kind, {"metadata": pod_meta, **state}))
        if status is not None and not self.scenario.get("status_first"):
            await self.queue("jobs").put(job_event)
        return "uid-1"

    async def own_secret(self, name: str, job: str, job_uid: str) -> None:
        self.calls.append(("own_secret", (name, job, job_uid)))

    async def delete_job(self, name: str, namespace: str) -> None:
        self.calls.append(("delete_job", (name, namespace)))

    async def delete_secret(self, name: str, namespace: str) -> None:
        self.calls.append(("delete_secret", (name, namespace)))

    def queue(self, kind: str) -> asyncio.Queue[tuple[str, dict[str, Any]]]:
        return self.queues.setdefault(kind, asyncio.Queue())

    async def watch(self, kind: str) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        yield "SYNC", {"items": []}
        while True:
            yield await self.queue(kind).get()

    async def follow_logs(self, pod: str) -> AsyncIterator[bytes]:
        self.calls.append(("follow_logs", pod))
        for line in self.scenario.get("logs", []):
            yield f"{line}\n".encode()
        if self.scenario.get("follow_fails"):
            raise RuntimeError("the log stream broke")
        await asyncio.sleep(self.scenario.get("logs_hang", 0))

    async def last_line(self, pod: str, limit: int) -> str:
        if self.scenario.get("tail_fails"):
            raise RuntimeError("the API server is unreachable")
        self.calls.append(("last_line", (pod, limit)))
        lines = self.scenario.get("final_logs", self.scenario.get("logs", []))
        return (lines[-1] if lines else "").encode()[:limit].decode(errors="ignore")

    def named(self, call: str) -> list[Any]:
        return [args for name, args in self.calls if name == call]


class FakeContext:
    """A service's context: what it told."""

    def __init__(self) -> None:
        self.statuses: list[str] = []
        self.points: list[tuple[str, float, dict[str, str]]] = []

    async def status(self, phase: str, message: str | None = None) -> None:
        self.statuses.append(phase)

    async def metric(self, name: str, value: float, **labels: str) -> None:
        self.points.append((name, value, labels))


class Running:
    """The service on a fake cluster, in a thread of its own, answering calls on sockets."""

    def __init__(self, cluster: FakeCluster) -> None:
        self.cluster = cluster
        self.context = FakeContext()
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.service = load().KubernetesService(cluster)
        self.held: set[asyncio.Future[None]] = set()
        self.writers: list[asyncio.StreamWriter] = []
        self.run(self.service.start(self.context))

    def hold(self, work: Any) -> None:
        task = asyncio.ensure_future(work)
        self.held.add(task)
        task.add_done_callback(self.held.discard)

    def run(self, work: Any) -> Any:
        return asyncio.run_coroutine_threadsafe(work, self.loop).result(10)

    def connect(self) -> int:
        """A plugin's end of a new connection to the service."""
        plugin_end, service_end = socket.socketpair()
        self.run(self._serve(service_end))
        return plugin_end.detach()

    async def _serve(self, sock: socket.socket) -> None:
        sock.setblocking(False)
        reader, writer = await asyncio.open_unix_connection(sock=sock)
        self.writers.append(writer)
        # Each connection an attempt of its own, as the worker's connects say.
        connected = ServiceAttempt(f"a-{len(self.writers)}", "kubernetes", "1.0")

        async def call(message: dict[str, Any]) -> None:
            try:
                result = await self.service.on_call(
                    connected, message["method"], message.get("args")
                )
                answer = {"type": "answer", "id": message["id"], "result": result}
            except Exception as e:
                answer = {"type": "call_failed", "id": message["id"], "message": f"{e}"}
            writer.write(_frame(answer))
            await writer.drain()

        async def reading() -> None:
            while (message := await _read_frame(reader)) is not None:
                self.hold(call(message))

        self.hold(reading())

    def stop(self) -> None:
        self.run(self.service.stop())
        self.run(self._close())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)

    async def _close(self) -> None:
        for task in list(self.held):
            task.cancel()
        for writer in self.writers:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


def request(payload: object, cleanup: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    message: dict[str, Any] = {
        "type": "extension_task",
        "dag_id": "pods",
        "task": {
            "id": "Report_Job",
            "kind": "extension",
            "name": "kubernetes",
            "version": "1.0",
            "payload": payload,
            "settings": {"max_output_size": 4096},
        },
        "run": {"run_id": "r-1", "try_number": 1},
        "secrets": [{"name": {"path": "k8s/api", "key": "token"}, "value": "s3cret"}],
    }
    if cleanup is not None:
        message["cleanup"] = cleanup
    return message


# What a plugin tells the worker without waiting for an answer.
REPORTS = {"status", "link", "metrics", "notify"}


def serve(
    message: dict[str, Any],
    cluster: FakeCluster,
    on_message: Any = None,
    argv: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Runs the plugin on this thread, the main one as in a plugin's process (it installs a
    SIGTERM handler), the worker on another and the service on a third; what the plugin sent
    after `welcome`."""
    plugin = load()
    running = Running(cluster)
    worker_end, plugin_end = socket.socketpair()
    sent: list[dict[str, Any]] = []
    failures: list[BaseException] = []

    def worker() -> None:
        channel = Channel(worker_end)
        try:
            assert channel.receive()["type"] == "hello"
            channel.send({"type": "welcome"})
            channel.send(message)
            while True:
                got = channel.receive()
                sent.append(got)
                if got["type"] in ("task_succeeded", "task_failed"):
                    return
                if on_message is not None:
                    on_message(got)
                # Reports have no answer (decision 513); requests have theirs.
                if got["type"] not in REPORTS:
                    channel.send({"type": "handle_added"})
        except BaseException as e:  # reported on the test's thread
            failures.append(e)
        finally:
            channel.close()

    thread = threading.Thread(target=worker)
    thread.start()
    env = {
        "SCHEDR_AGENT_FD": str(plugin_end.detach()),
        "SCHEDR_SERVICE_FD": str(running.connect()),
    }
    original = plugin.main
    plugin.main = lambda *args, **kwargs: original(*args, **{**kwargs, "environ": env})
    handler = signal.getsignal(signal.SIGTERM)
    try:
        code = plugin.entry(argv or ["--namespace", "batch"])
    finally:
        signal.signal(signal.SIGTERM, handler)
        thread.join(10)
        running.stop()
    if failures:
        raise failures[0]
    assert code == (0 if sent[-1]["type"] == "task_succeeded" else 1)
    return sent


SUCCEEDS = {
    "logs": ["working", 'SCHEDR_OUTPUT: {"rows": 3}'],
    "pods": [pod("Pending"), pod("Running"), pod("Succeeded")],
    "status": {"succeeded": 1},
}


def test_an_attempt_is_a_job_its_log_streamed_and_its_last_line_the_output(
    capfd: pytest.CaptureFixture[str],
) -> None:
    cluster = FakeCluster(SUCCEEDS)
    created_before_handle: list[bool] = []

    def handle_added(message: dict[str, Any]) -> None:
        if message["type"] == "handle_add":
            created_before_handle.append(bool(cluster.named("create_job")))

    payload = {
        "image": "busybox:1.36",
        "command": ["sh", "-c", "echo hi"],
        "env": {"MODE": "fast"},
        "secret_env": {"TOKEN": "k8s/api#token"},
        "node_selector": {"pool": "batch"},
    }
    sent = serve(request(payload), cluster, handle_added)
    assert sent[-1] == {"type": "task_succeeded", "output": {"rows": 3}, "outcome": "SUCCESS"}
    (handle,) = (m for m in sent if m["type"] == "handle_add")
    job = handle["handle"]["value"]["name"]
    assert handle["handle"] == {"handle_type": "job", "value": {"name": job, "namespace": "batch"}}
    assert job.startswith("schedr-report-job-")
    assert len(job) <= 63  # a DNS label
    assert created_before_handle == [False], "the handle comes before the Job"
    # Decision 513: its phases and its metrics, as the page declares them.
    phases = [m["phase"] for m in sent if m["type"] == "status"]
    assert phases == ["creating", "pending", "running", "succeeded"]
    reported = {p["name"] for m in sent if m["type"] == "metrics" for p in m["points"]}
    assert {
        "jobs_started",
        "jobs_by_image",
        "start_seconds",
        "job_seconds",
        "jobs_ended",
    } <= reported
    (manifest,) = cluster.named("create_job")
    assert manifest["metadata"]["name"] == job
    assert manifest["spec"]["backoffLimit"] == 0
    template = manifest["spec"]["template"]
    assert template["metadata"]["labels"] == {"app.kubernetes.io/managed-by": "schedr"}
    spec = template["spec"]
    assert spec["restartPolicy"] == "Never"
    assert spec["automountServiceAccountToken"] is False, "no API token in a task's pod"
    assert spec["nodeSelector"] == {"pool": "batch"}
    (container,) = spec["containers"]
    assert container["command"] == ["sh", "-c", "echo hi"]
    # The secret's value in a Secret of its own, made the Job's: never in the Job.
    assert container["env"] == [
        {"name": "MODE", "value": "fast"},
        {"name": "TOKEN", "valueFrom": {"secretKeyRef": {"name": f"{job}-env", "key": "TOKEN"}}},
    ]
    assert "s3cret" not in json.dumps(manifest)
    assert cluster.named("create_secret") == [(f"{job}-env", {"TOKEN": "s3cret"})]
    assert cluster.named("own_secret") == [(f"{job}-env", job, "uid-1")]
    assert "working" in capfd.readouterr().out


def test_a_job_without_secret_variables_has_no_secret() -> None:
    cluster = FakeCluster(SUCCEEDS)
    serve(request({"image": "busybox:1.36"}), cluster)
    assert cluster.named("create_secret") == []
    assert cluster.named("own_secret") == []


def test_another_try_is_another_job() -> None:
    plugin = load()
    first = plugin.ExtensionRequest.from_message(request({"image": "x"}))
    again = request({"image": "x"})
    again["run"]["try_number"] = 2
    assert plugin.job_name(first) != plugin.job_name(plugin.ExtensionRequest.from_message(again))


@pytest.mark.parametrize(
    ("status", "pods", "why"),
    [
        (
            {"failed": 1},
            [pod("Running"), pod("Failed", {"terminated": {"reason": "Error", "exitCode": 3}})],
            "the pod ended Error with exit code 3",
        ),
        (
            None,
            [pod("Pending", {"waiting": {"reason": "ErrImagePull", "message": "no such image"}})],
            "ErrImagePull: no such image",
        ),
        ({"failed": 1}, [], "the Job failed before its pod started"),
    ],
)
def test_a_failed_or_stuck_pod_fails_the_attempt_with_its_reason(
    status: dict[str, Any] | None, pods: list[Any], why: str
) -> None:
    cluster = FakeCluster({"status": status, "pods": pods})
    sent = serve(request({"image": "busybox:1.36"}), cluster)
    assert sent[-1]["type"] == "task_failed"
    assert sent[-1]["error"]["exception"] == "PodFailedError"
    assert sent[-1]["error"]["message"] == why


def test_a_pod_whose_image_cant_be_pulled_is_told_as_its_declared_event() -> None:
    waiting = {"waiting": {"reason": "ImagePullBackOff", "message": "back-off"}}
    sent = serve(
        request({"image": "busybox:9.99"}), FakeCluster({"pods": [pod("Pending", waiting)]})
    )
    told = [m for m in sent if m["type"] == "notify"]
    assert len(told) == 1, sent
    assert told[0]["event"] == "image_pull_failed"
    assert told[0]["data"]["reason"] == "ImagePullBackOff"
    assert told[0]["text"].endswith("ImagePullBackOff: back-off")
    assert sent[-1]["type"] == "task_failed"
    declared = {n["name"] for n in load().NOTIFICATIONS}
    assert told[0]["event"] in declared


def test_a_failed_pod_after_its_start_is_no_event() -> None:
    terminated = {"terminated": {"reason": "Error", "exitCode": 3}}
    cluster = FakeCluster(
        {"status": {"failed": 1}, "pods": [pod("Running"), pod("Failed", terminated)]}
    )
    sent = serve(request({"image": "busybox:1.36"}), cluster)
    assert [m for m in sent if m["type"] == "notify"] == []


def test_a_payload_without_an_image_fails_before_anything_is_created() -> None:
    cluster = FakeCluster({})
    sent = serve(request({"command": ["true"]}), cluster)
    assert sent[-1]["type"] == "task_failed"
    assert cluster.named("create_job") == []


def test_the_cleanup_deletes_each_jobs_handle_in_its_namespace() -> None:
    handles = [
        {"handle_type": "job", "value": {"name": "schedr-a-1", "namespace": "batch"}},
        {"handle_type": "job", "value": {"name": "schedr-a-2", "namespace": "original"}},
    ]
    cluster = FakeCluster({})
    sent = serve(request({"image": "x"}, cleanup=handles), cluster, argv=["--namespace", "other"])
    assert sent[-1]["output"] == {"deleted": ["schedr-a-1", "schedr-a-2"]}
    assert cluster.named("delete_job") == [("schedr-a-1", "batch"), ("schedr-a-2", "original")]
    assert cluster.named("delete_secret") == [
        ("schedr-a-1-env", "batch"),
        ("schedr-a-2-env", "original"),
    ]


def stop(reason: str) -> tuple[list[dict[str, Any]], int, FakeCluster]:
    """A running attempt sent SIGTERM, the worker answering its question with `reason`: what
    the plugin sent after its handle, its exit code, and the cluster."""
    cluster = FakeCluster({"logs": ["started"], "logs_hang": 60, "pods": [pod("Running")]})
    running = Running(cluster)
    service_fd = running.connect()
    worker_end, plugin_end = socket.socketpair()
    worker = Channel(worker_end)
    process = subprocess.Popen(
        [sys.executable, str(PLUGIN), "--namespace", "batch"],
        env={
            **os.environ,
            "SCHEDR_AGENT_FD": str(plugin_end.fileno()),
            "SCHEDR_SERVICE_FD": str(service_fd),
        },
        pass_fds=[plugin_end.fileno(), service_fd],
        stdout=subprocess.PIPE,
        start_new_session=True,
    )
    plugin_end.close()
    os.close(service_fd)
    sent: list[dict[str, Any]] = []
    try:
        assert worker.receive()["type"] == "hello"
        worker.send({"type": "welcome"})
        worker.send(request({"image": "busybox:1.36"}))
        assert worker.receive()["type"] == "handle_add"
        worker.send({"type": "handle_added"})
        deadline = time.monotonic() + 10
        while not cluster.named("follow_logs"):
            assert time.monotonic() < deadline, cluster.calls
            time.sleep(0.05)
        process.send_signal(signal.SIGTERM)
        while True:
            got = worker.receive()
            sent.append(got)
            if got["type"] == "stop_reason_get":
                worker.send({"type": "stop_reason", "reason": reason, "grace_ms": 29000})
            if got["type"] in ("task_succeeded", "task_failed"):
                break
        code = process.wait(10)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        worker.close()
        running.stop()
    return sent, code, cluster


def test_a_stopped_attempt_asks_why_and_deletes_its_job() -> None:
    """SIGTERM (a cancel, a timeout, a drain) while the pod runs: asked, the Job deleted."""
    sent, code, cluster = stop("cancel")
    assert code == 1
    assert "stop_reason_get" in [m["type"] for m in sent]
    ended = [m for m in sent if m["type"] == "metrics"][-1]["points"][0]
    assert ended == {"name": "jobs_ended", "value": 1.0, "labels": {"outcome": "stopped"}}
    (deleted,) = cluster.named("delete_job")
    assert deleted[0].startswith("schedr-report-job-")


def test_a_lost_master_leaves_the_job_to_the_cleanup() -> None:
    """Its cleanup runs from the handle, wherever the attempt's cleanup goes."""
    _, code, cluster = stop("master_lost")
    assert code == 1
    assert cluster.named("delete_job") == []


def test_describe_names_the_extension(capsys: pytest.CaptureFixture[str]) -> None:
    assert load().entry(["--describe"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "name": "kubernetes",
        "versions": ["1.0"],
        "protocol": 1,
    }


def test_a_broken_log_never_makes_the_output() -> None:
    """Codex's review of M5.2i2: the output is read from the ended pod, not from the log seen."""
    broken = {
        **SUCCEEDS,
        "logs": ['SCHEDR_OUTPUT: {"rows": 1}'],
        "follow_fails": True,
        "final_logs": ['SCHEDR_OUTPUT: {"rows": 1}', 'SCHEDR_OUTPUT: {"rows": 3}'],
    }
    assert serve(request({"image": "x"}), FakeCluster(broken))[-1]["output"] == {"rows": 3}
    failed = serve(request({"image": "x"}), FakeCluster({**SUCCEEDS, "tail_fails": True}))[-1]
    assert failed["type"] == "task_failed"
    assert "unreachable" in failed["error"]["message"]


@pytest.mark.parametrize(
    "payload",
    [
        {"image": "x", "command": "echo expected"},
        {"image": "x", "command": ["sh", 3]},
        {"image": "x", "env": ["MODE=fast"]},
        {"image": "x", "env": {"MODE": 1}},
        {"image": "x", "secret_env": "etl/api#token"},
        {"image": "x", "comand": ["true"]},
        {"image": 7},
        ["x"],
    ],
)
def test_a_mistyped_payload_fails_before_anything_is_created(payload: object) -> None:
    cluster = FakeCluster({})
    assert serve(request(payload), cluster)[-1]["type"] == "task_failed"
    assert cluster.named("create_job") == []


def test_a_long_log_line_is_written_whole(capfd: pytest.CaptureFixture[str]) -> None:
    """agy's review: it checked only the output, never the log written."""
    long = "x" * 200_000
    cluster = FakeCluster({**SUCCEEDS, "logs": [long, 'SCHEDR_OUTPUT: {"rows": 3}']})
    assert serve(request({"image": "x"}), cluster)[-1]["output"] == {"rows": 3}
    assert f"{long}\n" in capfd.readouterr().out


def test_the_service_reports_the_namespaces_pods_as_gauges() -> None:
    """From its watch: running and pending, and by phase, told when they change."""
    cluster = FakeCluster({})
    running = Running(cluster)
    try:

        async def add() -> None:
            for name, phase in [("a", "Running"), ("b", "Running"), ("c", "Pending")]:
                await cluster.queue("pods").put(
                    ("ADDED", {"metadata": {"name": name}, "status": {"phase": phase}})
                )

        running.run(add())
        deadline = time.monotonic() + 5
        while ("pods_running", 2.0, {}) not in running.context.points:
            assert time.monotonic() < deadline, running.context.points
            time.sleep(0.05)
        points = running.context.points
        assert ("pods_pending", 1.0, {}) in points
        assert ("pods", 2.0, {"phase": "Running"}) in points
        assert running.context.statuses == ["watching"]
    finally:
        running.stop()
    assert cluster.closed


def test_an_unknown_call_fails_and_a_restarted_watch_forgets_what_it_held() -> None:
    plugin = load()
    cluster = FakeCluster({})
    service = plugin.KubernetesService(cluster)

    async def check() -> None:
        with pytest.raises(ValueError, match="unknown method"):
            await service.on_call(None, "explode", None)
        watched = plugin.Watched()

        async def events() -> AsyncIterator[tuple[str, dict[str, Any]]]:
            yield "ADDED", {"metadata": {"name": "a"}}
            yield "SYNC", {"items": [{"metadata": {"name": "c"}}]}
            yield "ADDED", {"metadata": {"name": "b"}}
            yield "DELETED", {"metadata": {"name": "b"}}

        await watched.follow(events())
        # A watch listed again replaces what it held: one gone meanwhile is remembered deleted.
        assert list(watched.objects) == ["c"]
        assert list(watched.deleted) == ["a", "b"]

    asyncio.run(check())


def test_links_go_to_headlamps_pages_or_the_dashboards() -> None:
    # M5.2i7: Headlamp's routes (0.46.0: /c/<cluster>/pods/:namespace/:name), the Dashboard's.
    plugin = load()
    headlamp = plugin.Ui("headlamp", "http://127.0.0.1:4466/")
    assert headlamp.page_url("pod", "batch", "p-1") == "http://127.0.0.1:4466/c/main/pods/batch/p-1"
    assert headlamp.page_url("job", "batch", "j-1") == "http://127.0.0.1:4466/c/main/jobs/batch/j-1"
    other = plugin.Ui("headlamp", "https://ui", "prod")
    assert other.page_url("pod", "a", "b") == "https://ui/c/prod/pods/a/b"
    dashboard = plugin.Ui("dashboard", "https://dash")
    assert dashboard.page_url("job", "a", "b") == "https://dash/#/job/a/b?namespace=a"


def test_its_options_choose_the_ui_and_the_mode() -> None:
    # agy's review of M5.2i7: `--headlamp` and `--dashboard` were tested only as `Ui`s.
    plugin = load()
    options = plugin.parsed(["--namespace", "batch", "--headlamp", "http://h:1"])
    ui = plugin.ui_of(options)
    assert (options.namespace, options.service, ui.kind, ui.address, ui.cluster) == (
        "batch",
        False,
        "headlamp",
        "http://h:1",
        "main",
    )
    ui = plugin.ui_of(plugin.parsed(["--headlamp", "http://h", "--headlamp-cluster", "prod"]))
    assert ui.page_url("pod", "a", "p") == "http://h/c/prod/pods/a/p"
    assert plugin.ui_of(plugin.parsed(["--dashboard", "https://d"])).kind == "dashboard"
    assert plugin.ui_of(plugin.parsed([])) is None
    assert plugin.parsed(["--service", "--namespace", "x"]).service
    with pytest.raises(SystemExit):
        plugin.parsed(["--headlamp", "http://h", "--dashboard", "https://d"])


def test_the_manifest_labels_its_pod_and_reads_secrets_from_the_jobs_secret() -> None:
    manifest = load().manifest("j", {"image": "x", "env": {"A": "1"}}, ["T"])
    labels = manifest["spec"]["template"]["metadata"]["labels"]
    assert labels == {"app.kubernetes.io/managed-by": "schedr"}
    env = manifest["spec"]["template"]["spec"]["containers"][0]["env"]
    assert env[1]["valueFrom"]["secretKeyRef"] == {"name": "j-env", "key": "T"}


def test_the_real_cluster_is_made_before_its_loop_and_opened_in_it() -> None:
    """Found on kind: its client made outside the event loop crashed the service at each start."""
    cluster = load().KubeCluster("batch", None, None)
    with pytest.raises(RuntimeError, match="isn't opened"):
        _ = cluster.core
    asyncio.run(cluster.close())


def attempt(attempt_id: str = "a-1") -> ServiceAttempt:
    return ServiceAttempt(attempt_id, "kubernetes", "1.0")


def test_a_pod_that_never_starts_ends_its_job_failed_and_an_unread_output_never_succeeds() -> None:
    """agy's review: a pod that never started was never counted as ended; one whose output
    couldn't be read was told succeeded first."""
    waiting = {"waiting": {"reason": "ErrImagePull", "message": "no such image"}}
    sent = serve(request({"image": "x"}), FakeCluster({"pods": [pod("Pending", waiting)]}))
    assert [m["phase"] for m in sent if m["type"] == "status"][-1] == "failed"
    ended = [
        p for m in sent if m["type"] == "metrics" for p in m["points"] if p["name"] == "jobs_ended"
    ]
    assert ended == [{"name": "jobs_ended", "value": 1.0, "labels": {"outcome": "failed"}}]
    sent = serve(request({"image": "x"}), FakeCluster({**SUCCEEDS, "tail_fails": True}))
    assert "succeeded" not in [m["phase"] for m in sent if m["type"] == "status"]


def test_a_failed_jobs_reason_is_waited_for_when_its_watch_comes_first() -> None:
    """agy's review: the Jobs' watch ahead of the pods' lost the pod's exit code."""
    terminated = {"terminated": {"reason": "Error", "exitCode": 3}}
    scenario = {
        "status": {"failed": 1},
        "status_first": True,
        "pods": [pod("Running"), pod("Failed", terminated)],
    }
    sent = serve(request({"image": "x"}), FakeCluster(scenario))
    assert sent[-1]["error"]["message"] == "the pod ended Error with exit code 3"


def test_a_job_deleted_from_outside_ends_the_wait_for_it() -> None:
    """agy's review: `started` and `ended` waited for ever for a Job deleted meanwhile."""
    cluster = FakeCluster({})
    running = Running(cluster)
    try:
        running.run(cluster.queue("jobs").put(("DELETED", {"metadata": {"name": "j-1"}})))
        answer = running.run(running.service.on_call(attempt(), "started", {"job": "j-1"}))
        assert answer == {"failed": "the Job was deleted", "waiting": None}
    finally:
        running.stop()


def test_a_deleted_job_is_never_created_and_a_failed_step_undoes_the_others() -> None:
    """agy's review: a stop during `submit` deleted nothing and the Job was created after; a
    Job failing to be created left its Secret."""
    cluster = FakeCluster({})
    running = Running(cluster)
    try:
        service = running.service
        running.run(service.on_call(attempt(), "delete", {"job": "j-1"}))
        submit = {"job": "j-1", "payload": {"image": "x"}, "secrets": {}}
        with pytest.raises(Exception, match="deleted before it was created"):
            running.run(service.on_call(attempt(), "submit", submit))
        assert cluster.named("create_job") == []
    finally:
        running.stop()
    cluster = FakeCluster({"create_fails": True})
    running = Running(cluster)
    try:
        submit = {"job": "j-2", "payload": {"image": "x"}, "secrets": {"T": "v"}}
        with pytest.raises(RuntimeError, match="invalid"):
            running.run(running.service.on_call(attempt(), "submit", submit))
        assert cluster.named("delete_secret") == [("j-2-env", "batch")]
    finally:
        running.stop()


def test_an_attempts_followed_log_is_let_go_with_its_connection() -> None:
    """agy's review: a plugin gone left its pod's log followed for ever."""
    cluster = FakeCluster({"logs": ["a"], "logs_hang": 60})
    running = Running(cluster)
    try:
        service = running.service
        running.run(service.on_call(attempt("a-1"), "logs", {"pod": "p-1"}))
        assert list(service.logs) == ["p-1"]
        tail = service.logs["p-1"]
        running.run(service.on_closed(attempt("a-1")))
        assert service.logs == {}
        deadline = time.monotonic() + 5
        while not tail.task.done():
            assert time.monotonic() < deadline
            time.sleep(0.05)
    finally:
        running.stop()


def test_a_refused_log_is_an_error_and_a_wrong_kubeconfig_is_told() -> None:
    """agy's review: an HTTP error on the followed log was read as the log; a wrong
    `--kubeconfig` fell back to the pod's own service account."""
    plugin = load()

    class Refused:
        status = 403

        async def text(self) -> str:
            return "pods is forbidden"

        async def __aenter__(self) -> Refused:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

    class Core:
        async def read_namespaced_pod_log(self, *_: object, **__: object) -> Refused:
            return Refused()

    cluster = plugin.KubeCluster("batch", None, None)
    cluster.clients = (None, Core(), None)

    async def follow() -> None:
        async for _ in cluster.follow_logs("p-1"):
            pass

    with pytest.raises(RuntimeError, match="HTTP 403: pods is forbidden"):
        asyncio.run(follow())
    wrong = plugin.KubeCluster("batch", None, "/nonexistent/kubeconfig")
    with pytest.raises(FileNotFoundError, match="no kubeconfig at /nonexistent"):
        asyncio.run(wrong.open())


def test_an_output_line_is_read_no_further_than_the_tasks_max_output_size() -> None:
    """Grok's review: the last line was read whole, so one huge line took the shared service
    past its memory limit."""
    big = "SCHEDR_OUTPUT: " + json.dumps({"rows": "x" * 5000})
    cluster = FakeCluster({**SUCCEEDS, "logs": [big]})
    sent = serve(request({"image": "x"}), cluster)
    assert [limit for _, limit in cluster.named("last_line")] == [4097]
    assert sent[-1]["type"] == "task_failed"
    assert "the output line is over 4096 bytes" in sent[-1]["error"]["message"]
