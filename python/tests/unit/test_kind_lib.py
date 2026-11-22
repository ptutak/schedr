"""scripts/kind-lib.sh's `require_local_context`: the kind scripts talk only to a cluster on this
machine (agy's review of M5.2i7: a context's cluster read by the context's name, and a server
`https://127.0.0.1:1@host` taken for local). Run against crafted kubeconfigs with `kubectl`."""

import subprocess
from pathlib import Path

import pytest

LIB = Path(__file__).parents[3] / "scripts" / "kind-lib.sh"


def kubeconfig(path: Path, context_cluster: str, server: str, extra: str = "") -> Path:
    """A kubeconfig whose context `kind-demo` names `context_cluster` at `server`."""
    path.write_text(
        f"""apiVersion: v1
kind: Config
clusters:
- name: {context_cluster}
  cluster: {{server: "{server}"}}
{extra}contexts:
- name: kind-demo
  context: {{cluster: {context_cluster}, user: u}}
users:
- name: u
  user: {{}}
"""
    )
    return path


def allowed(config: Path) -> bool:
    """Whether `require_local_context` lets the context through."""
    script = f'source "{LIB}"; CONTEXT=kind-demo; require_local_context'
    done = subprocess.run(
        ["bash", "-c", script],
        env={"KUBECONFIG": str(config), "PATH": "/usr/local/bin:/usr/bin:/bin"},
        capture_output=True,
        check=False,
    )
    return done.returncode == 0


@pytest.mark.parametrize("server", ["https://127.0.0.1:41234", "https://localhost:6443/"])
def test_a_local_cluster_is_allowed(tmp_path: Path, server: str) -> None:
    assert allowed(kubeconfig(tmp_path / "c.yaml", "kind-demo", server))


@pytest.mark.parametrize(
    "server",
    [
        "https://127.0.0.1:1@prod.example",
        "https://localhost:6443/../x",
        "https://127.0.0.1.prod.example:443",
        "https://prod.example:443",
        "",
    ],
)
def test_a_server_that_isnt_plainly_local_is_refused(tmp_path: Path, server: str) -> None:
    assert not allowed(kubeconfig(tmp_path / "c.yaml", "kind-demo", server))


def test_the_contexts_own_cluster_is_read_not_one_named_like_it(tmp_path: Path) -> None:
    stale = '- name: kind-demo\n  cluster: {server: "https://127.0.0.1:41234"}\n'
    config = kubeconfig(tmp_path / "c.yaml", "prod-eks", "https://prod.eks.example:443", stale)
    assert not allowed(config)
