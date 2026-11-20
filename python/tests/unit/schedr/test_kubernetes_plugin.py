"""The Kubernetes example plugin (M5.2i2) against a fake `kubectl`: what it creates, streams,
returns and deletes. The real thing on kind: `scripts/kind-extension.sh`."""

import contextlib
import importlib.machinery
import importlib.util
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from schedr.runner.protocol import Channel

PLUGIN = (
    Path(__file__).parents[4] / "examples" / "extensions" / "kubernetes" / "schedr-ext-kubernetes"
)

# A fake kubectl: each call appended to calls.jsonl; what it answers read from scenario.json.
FAKE = textwrap.dedent(
    """\
    #!{python}
    import json, sys, time
    from pathlib import Path
    here = Path(__file__).parent
    args = sys.argv[1:]
    stdin = sys.stdin.read() if "-f" in args and "-" in args else None
    with (here / "calls.jsonl").open("a") as calls:
        calls.write(json.dumps({{"args": args, "stdin": stdin}}) + "\\n")
    scenario = json.loads((here / "scenario.json").read_text())
    if "logs" in args and "-f" in args:
        sys.stderr.write("e" * scenario.get("stderr_bytes", 0))
        sys.stderr.flush()
        for line in scenario.get("logs", []):
            print(line, flush=True)
        time.sleep(scenario.get("logs_hang", 0))
        sys.exit(scenario.get("follow_exit", 0))
    elif "logs" in args:
        if scenario.get("tail_fails"):
            sys.stderr.write("the API server is unreachable")
            sys.exit(1)
        lines = scenario.get("final_logs", scenario.get("logs", []))
        print(lines[-1] if lines else "")
    elif "get" in args and args[args.index("get") + 1] == "job":
        print(json.dumps({{"status": scenario.get("status", {{}})}}))
    elif "get" in args:
        print(json.dumps({{"items": scenario.get("pods", [])}}))
    """
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


@pytest.fixture
def kubectl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The fake on `PATH`; the test writes its scenario."""
    fake = tmp_path / "kubectl"
    fake.write_text(FAKE.format(python=sys.executable))
    fake.chmod(0o755)
    (tmp_path / "scenario.json").write_text("{}")
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    return tmp_path


def pod(phase: str, state: dict[str, Any] | None = None) -> dict[str, Any]:
    """A pod as `kubectl get pods -o json` lists it."""
    statuses = [{"state": state}] if state is not None else []
    return {
        "metadata": {"name": "pod-1"},
        "status": {"phase": phase, "containerStatuses": statuses},
    }


def scenario(dir: Path, **values: object) -> None:
    (dir / "scenario.json").write_text(json.dumps(values))


def calls(dir: Path) -> list[dict[str, Any]]:
    path = dir / "calls.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


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
REPORTS = {"status", "link", "metrics"}


def serve(
    message: dict[str, Any], on_message: Any = None, argv: list[str] | None = None
) -> list[dict[str, Any]]:
    """Runs the plugin on this thread, the main one as in a plugin's process (it installs a
    SIGTERM handler), and the worker on another; what the plugin sent after `welcome`."""
    plugin = load()
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
    env = {"SCHEDR_AGENT_FD": str(plugin_end.detach())}
    original = plugin.main
    plugin.main = lambda *args, **kwargs: original(*args, **{**kwargs, "environ": env})
    handler = signal.getsignal(signal.SIGTERM)
    try:
        code = plugin.entry(argv or ["--namespace", "batch"])
    finally:
        signal.signal(signal.SIGTERM, handler)
        thread.join(10)
    if failures:
        raise failures[0]
    assert code == (0 if sent[-1]["type"] == "task_succeeded" else 1)
    return sent


def test_an_attempt_is_a_job_its_log_streamed_and_its_last_line_the_output(
    kubectl: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    scenario(
        kubectl,
        logs=["working", 'SCHEDR_OUTPUT: {"rows": 3}'],
        status={"succeeded": 1},
        pods=[pod("Succeeded")],
    )
    created_before_handle: list[bool] = []

    def handle_added(message: dict[str, Any]) -> None:
        if message["type"] == "handle_add":
            created_before_handle.append(any("create" in c["args"] for c in calls(kubectl)))

    payload = {
        "image": "busybox:1.36",
        "command": ["sh", "-c", "echo hi"],
        "env": {"MODE": "fast"},
        "secret_env": {"TOKEN": "k8s/api#token"},
        "node_selector": {"pool": "batch"},
    }
    sent = serve(request(payload), handle_added)
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
    assert {"jobs_started", "start_seconds", "job_seconds", "jobs_ended"} <= reported
    (create,) = (c for c in calls(kubectl) if "create" in c["args"])
    assert create["args"] == ["-n", "batch", "create", "-f", "-"]
    job_manifest = json.loads(create["stdin"])
    assert job_manifest["metadata"]["name"] == job
    assert job_manifest["spec"]["backoffLimit"] == 0
    spec = job_manifest["spec"]["template"]["spec"]
    assert spec["restartPolicy"] == "Never"
    assert spec["automountServiceAccountToken"] is False, "no API token in a task's pod"
    assert spec["nodeSelector"] == {"pool": "batch"}
    (container,) = spec["containers"]
    assert container["command"] == ["sh", "-c", "echo hi"]
    assert container["env"] == [
        {"name": "MODE", "value": "fast"},
        {"name": "TOKEN", "value": "s3cret"},
    ]
    assert "working" in capfd.readouterr().out


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
            [pod("Failed", {"terminated": {"reason": "Error", "exitCode": 3}})],
            "the pod ended Error with exit code 3",
        ),
        (
            {},
            [pod("Pending", {"waiting": {"reason": "ErrImagePull", "message": "no such image"}})],
            "ErrImagePull: no such image",
        ),
    ],
)
def test_a_failed_or_stuck_pod_fails_the_attempt_with_its_reason(
    kubectl: Path, status: dict[str, Any], pods: list[Any], why: str
) -> None:
    scenario(kubectl, status=status, pods=pods)
    sent = serve(request({"image": "busybox:1.36"}))
    assert sent[-1]["type"] == "task_failed"
    assert sent[-1]["error"]["exception"] == "PodFailedError"
    assert sent[-1]["error"]["message"] == why


def test_a_payload_without_an_image_fails_before_anything_is_created(kubectl: Path) -> None:
    sent = serve(request({"command": ["true"]}))
    assert sent[-1]["type"] == "task_failed"
    assert calls(kubectl) == []


def test_the_cleanup_deletes_each_jobs_handle(kubectl: Path) -> None:
    handles = [
        {"handle_type": "job", "value": {"name": "schedr-a-1", "namespace": "batch"}},
        {"handle_type": "job", "value": {"name": "schedr-a-2", "namespace": "batch"}},
    ]
    sent = serve(request({"image": "x"}, cleanup=handles))
    assert sent[-1]["output"] == {"deleted": ["schedr-a-1", "schedr-a-2"]}
    deletes = [c["args"] for c in calls(kubectl) if "delete" in c["args"]]
    assert [d[d.index("job") + 1] for d in deletes] == ["schedr-a-1", "schedr-a-2"]


def stop(kubectl: Path, reason: str) -> tuple[list[dict[str, Any]], int]:
    """A running attempt sent SIGTERM, the worker answering its question with `reason`: what
    the plugin sent after its handle, and its exit code."""
    scenario(kubectl, logs=["started"], logs_hang=60, pods=[pod("Running")])
    worker_end, plugin_end = socket.socketpair()
    worker = Channel(worker_end)
    process = subprocess.Popen(
        [sys.executable, str(PLUGIN), "--namespace", "batch"],
        env={**os.environ, "SCHEDR_AGENT_FD": str(plugin_end.fileno())},
        pass_fds=[plugin_end.fileno()],
        stdout=subprocess.PIPE,
        # A group of its own, as the worker starts it: what it started goes with it below.
        start_new_session=True,
    )
    plugin_end.close()
    sent: list[dict[str, Any]] = []
    try:
        assert worker.receive()["type"] == "hello"
        worker.send({"type": "welcome"})
        worker.send(request({"image": "busybox:1.36"}))
        assert worker.receive()["type"] == "handle_add"
        worker.send({"type": "handle_added"})
        deadline = time.monotonic() + 10
        while not any("logs" in c["args"] for c in calls(kubectl)):
            assert time.monotonic() < deadline, calls(kubectl)
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
        # As the worker does once a task ends (B274): its fake `kubectl logs` too.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        worker.close()
    return sent, code


def test_a_stopped_attempt_asks_why_and_deletes_its_job(kubectl: Path) -> None:
    """SIGTERM (a cancel, a timeout, a drain) while the pod runs: asked, the Job deleted."""
    sent, code = stop(kubectl, "cancel")
    assert code == 1
    assert "stop_reason_get" in [m["type"] for m in sent]
    ended = [m for m in sent if m["type"] == "metrics"][-1]["points"][0]
    assert ended == {"name": "jobs_ended", "value": 1.0, "labels": {"outcome": "stopped"}}
    deletes = [c["args"] for c in calls(kubectl) if "delete" in c["args"]]
    assert len(deletes) == 1
    assert deletes[0][deletes[0].index("job") + 1].startswith("schedr-report-job-")


def test_a_lost_master_leaves_the_job_to_the_cleanup(kubectl: Path) -> None:
    """Its cleanup runs from the handle, wherever the attempt's cleanup goes."""
    _, code = stop(kubectl, "master_lost")
    assert code == 1
    assert [c for c in calls(kubectl) if "delete" in c["args"]] == []


def test_describe_names_the_extension(capsys: pytest.CaptureFixture[str]) -> None:
    assert load().entry(["--describe"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "name": "kubernetes",
        "versions": ["1.0"],
        "protocol": 1,
    }


def test_a_broken_log_stream_never_makes_the_output(kubectl: Path) -> None:
    """Codex's review: the output is read from the ended pod, not from what the stream saw."""
    scenario(
        kubectl,
        logs=['SCHEDR_OUTPUT: {"rows": 1}'],
        follow_exit=1,
        final_logs=['SCHEDR_OUTPUT: {"rows": 1}', 'SCHEDR_OUTPUT: {"rows": 3}'],
        status={"succeeded": 1},
        pods=[pod("Succeeded")],
    )
    assert serve(request({"image": "x"}))[-1]["output"] == {"rows": 3}
    scenario(kubectl, logs=["x"], tail_fails=True, status={"succeeded": 1}, pods=[pod("Succeeded")])
    failed = serve(request({"image": "x"}))[-1]
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
def test_a_mistyped_payload_fails_before_anything_is_created(
    kubectl: Path, payload: object
) -> None:
    assert serve(request(payload))[-1]["type"] == "task_failed"
    assert calls(kubectl) == []


def test_the_cleanup_deletes_in_the_handles_namespace(kubectl: Path) -> None:
    handles = [{"handle_type": "job", "value": {"name": "schedr-a-1", "namespace": "original"}}]
    serve(request({"image": "x"}, cleanup=handles), argv=["--namespace", "replacement"])
    (delete,) = [c["args"] for c in calls(kubectl) if "delete" in c["args"]]
    assert delete[:2] == ["-n", "original"]


def test_a_long_log_line_and_a_noisy_kubectl_dont_fail_or_block_the_attempt(
    kubectl: Path,
) -> None:
    scenario(
        kubectl,
        logs=["x" * 200_000, 'SCHEDR_OUTPUT: {"rows": 3}'],
        stderr_bytes=1 << 20,
        status={"succeeded": 1},
        pods=[pod("Succeeded")],
    )
    assert serve(request({"image": "x"}))[-1]["output"] == {"rows": 3}
