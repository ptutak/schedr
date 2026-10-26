"""The supply-chain pin checker rejects bad workflows and accepts this repo."""

from __future__ import annotations

import importlib.util
from functools import lru_cache
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[3]
PIN = "a" * 40
DIGEST = "ab" * 32


@lru_cache(maxsize=1)
def pins() -> ModuleType:
    """Load scripts/check-action-pins.py."""
    path = ROOT / "scripts" / "check-action-pins.py"
    spec = importlib.util.spec_from_file_location("check_action_pins", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def yaml_errors(text: str) -> list[str]:
    """Pin errors for one workflow, against the channel 1.99.1."""
    return pins().check_yaml(text, "ci.yml", "1.99.1")


def assert_fails(text: str, snippet: str) -> None:
    """The workflow fails, and one error contains snippet."""
    errors = yaml_errors(text)
    assert errors, "expected a pin error"
    assert any(snippet in error for error in errors), errors


def test_unpinned_uses_names_the_line() -> None:
    text = "name: CI\non: push\njobs:\n  build:\n    uses: actions/checkout@v4\n"
    errors = yaml_errors(text)
    assert len(errors) == 1
    assert errors[0].startswith("ci.yml:5:")
    assert "actions/checkout@v4" in errors[0]


def test_flow_style_uses_fails() -> None:
    text = (
        "name: CI\non: push\njobs:\n  build:\n    steps:\n      - { uses: actions/checkout@v4 }\n"
    )
    assert_fails(text, "actions/checkout@v4")


def test_quoted_uses_key_fails() -> None:
    text = 'name: CI\non: push\njobs:\n  build:\n    steps:\n      - "uses": actions/checkout@v4\n'
    assert_fails(text, "actions/checkout@v4")


def test_uses_value_on_the_next_line_fails() -> None:
    text = (
        "name: CI\non: push\njobs:\n  build:\n    steps:\n"
        "      - uses:\n          actions/checkout@v4\n"
    )
    assert_fails(text, "actions/checkout@v4")


def test_toolchain_expression_fails() -> None:
    text = (
        "name: CI\non: push\njobs:\n  rust:\n    steps:\n"
        f"      - uses: dtolnay/rust-toolchain@{PIN}\n"
        "        with:\n"
        "          toolchain: ${{ matrix.toolchain }}\n"
    )
    assert_fails(text, "matrix.toolchain")


def test_toolchain_on_the_next_line_fails_when_it_is_not_the_channel() -> None:
    text = (
        "name: CI\non: push\njobs:\n  rust:\n    steps:\n"
        f"      - uses: dtolnay/rust-toolchain@{PIN}\n"
        "        with:\n"
        "          toolchain:\n"
        "            stable\n"
    )
    assert_fails(text, "stable")


def test_toolchain_on_the_next_line_passes_when_it_matches() -> None:
    text = (
        "name: CI\non: push\njobs:\n  rust:\n    steps:\n"
        f"      - uses: dtolnay/rust-toolchain@{PIN}\n"
        "        with:\n"
        "          toolchain:\n"
        '            "1.99.1"\n'
    )
    assert yaml_errors(text) == []


def test_flow_style_toolchain_fails() -> None:
    text = (
        "name: CI\non: push\njobs:\n  rust:\n    steps:\n"
        f"      - uses: dtolnay/rust-toolchain@{PIN}\n"
        "        with: { toolchain: stable }\n"
    )
    assert_fails(text, "stable")


def test_maturin_rust_toolchain_must_match_the_file() -> None:
    text = (
        "name: CI\non: push\njobs:\n  sdist:\n    steps:\n"
        f"      - uses: PyO3/maturin-action@{PIN}\n"
        "        with:\n"
        "          rust-toolchain: stable\n"
    )
    assert_fails(text, "rust-toolchain: stable")


def test_install_action_tool_needs_a_version() -> None:
    text = (
        "name: CI\non: push\njobs:\n  rust:\n    steps:\n"
        f"      - uses: taiki-e/install-action@{PIN}\n"
        "        with:\n"
        "          tool: cargo-nextest\n"
    )
    assert_fails(text, "cargo-nextest")


def test_install_action_rejects_latest() -> None:
    text = (
        "name: CI\non: push\njobs:\n  rust:\n    steps:\n"
        f"      - uses: taiki-e/install-action@{PIN}\n"
        "        with:\n"
        "          tool: cargo-nextest@latest\n"
    )
    assert_fails(text, "latest")


def test_setup_uv_needs_a_version() -> None:
    text = (
        "name: CI\non: push\njobs:\n  python:\n    steps:\n"
        f"      - uses: astral-sh/setup-uv@{PIN}\n"
        "        with:\n"
        '          python-version: "3.14"\n'
    )
    assert_fails(text, "version:")


def test_setup_uv_rejects_latest() -> None:
    text = (
        "name: CI\non: push\njobs:\n  python:\n    steps:\n"
        f"      - uses: astral-sh/setup-uv@{PIN}\n"
        "        with:\n"
        "          version: latest\n"
    )
    assert_fails(text, "latest")


def test_setup_helm_needs_a_version() -> None:
    text = (
        f"name: CI\non: push\njobs:\n  chart:\n    steps:\n      - uses: azure/setup-helm@{PIN}\n"
    )
    assert_fails(text, "version")


def test_local_actions_and_reusable_workflows_pass() -> None:
    text = (
        "name: CI\non: push\njobs:\n"
        "  call:\n"
        "    uses: ./.github/workflows/reusable.yml\n"
        "  build:\n"
        "    steps:\n"
        "      - uses: ./.github/actions/local\n"
    )
    assert yaml_errors(text) == []


def test_docker_uses_without_a_digest_fails() -> None:
    text = "name: CI\non: push\njobs:\n  build:\n    steps:\n      - uses: docker://alpine:3.20\n"
    assert_fails(text, "docker://alpine:3.20")


def test_docker_uses_with_a_digest_passes() -> None:
    text = (
        "name: CI\non: push\njobs:\n  build:\n    steps:\n"
        f"      - uses: docker://alpine@sha256:{DIGEST}\n"
    )
    assert yaml_errors(text) == []


def test_unpinned_service_image_fails() -> None:
    text = (
        "name: CI\non: push\njobs:\n  test:\n    services:\n"
        "      postgres:\n"
        "        image: docker.io/library/postgres:17.6-alpine\n"
        "    steps: []\n"
    )
    assert_fails(text, "postgres:17.6-alpine")


def test_flow_style_service_image_fails() -> None:
    text = (
        "name: CI\non: push\njobs:\n  test:\n"
        "    services:\n"
        "      db: { image: postgres:17 }\n"
        "    steps: []\n"
    )
    assert_fails(text, "postgres:17")


def test_pinned_service_image_passes() -> None:
    text = (
        "name: CI\non: push\njobs:\n  test:\n    services:\n"
        "      postgres:\n"
        f"        image: docker.io/library/postgres:17.6-alpine@sha256:{DIGEST}\n"
        "    steps: []\n"
    )
    assert yaml_errors(text) == []


def test_a_complete_pinned_workflow_passes() -> None:
    text = (
        "name: CI\non: push\njobs:\n  rust:\n    runs-on: ubuntu-latest\n"
        "    services:\n      postgres:\n"
        f"        image: docker.io/library/postgres:17.6-alpine@sha256:{DIGEST}\n"
        "    steps:\n"
        f"      - uses: actions/checkout@{PIN}\n"
        "      - uses: ./.github/actions/local\n"
        f"      - uses: docker://alpine@sha256:{DIGEST}\n"
        f"      - uses: dtolnay/rust-toolchain@{PIN}\n"
        "        with:\n"
        '          toolchain: "1.99.1"\n'
        f"      - uses: taiki-e/install-action@{PIN}\n"
        "        with:\n"
        "          tool: cargo-nextest@0.9.149,cargo-llvm-cov@0.9.1\n"
        f"      - uses: astral-sh/setup-uv@{PIN}\n"
        "        with:\n"
        '          version: "0.13.5"\n'
        f"      - uses: azure/setup-helm@{PIN}\n"
        "        with:\n"
        "          version: v4.3.0\n"
        "  call:\n"
        "    uses: ./.github/workflows/reusable.yml\n"
    )
    assert yaml_errors(text) == []


def test_requirement_without_a_hash_fails() -> None:
    errors = pins().check_requirements("maturin==1.15.0\n", "req.txt")
    assert len(errors) == 1
    assert errors[0].startswith("req.txt:1:")
    assert "maturin==1.15.0" in errors[0]


def test_requirement_with_a_hash_passes() -> None:
    text = f"maturin==1.15.0 \\\n    --hash=sha256:{DIGEST}\n"
    assert pins().check_requirements(text, "req.txt") == []


def test_shell_assignment_without_a_digest_fails() -> None:
    errors = pins().image_errors(
        "IMAGE=quay.io/pypa/manylinux2014_x86_64:2026.10.16-1\n",
        "build.sh",
    )
    assert len(errors) == 1
    assert errors[0].startswith("build.sh:1:")
    assert "not pinned" in errors[0]


def test_shell_assignment_with_a_digest_passes() -> None:
    text = f"IMAGE=quay.io/pypa/manylinux2014_x86_64@sha256:{DIGEST}\n"
    assert pins().image_errors(text, "build.sh") == []


def test_https_url_is_not_an_image_pull() -> None:
    text = "curl -O https://quay.io/pypa/manylinux2014_x86_64:2026.10.16-1\n"
    assert pins().image_errors(text, "build.sh") == []


def test_from_short_name_fails_and_scratch_passes() -> None:
    errors = pins().image_errors("FROM python:3.14\n", "Dockerfile")
    assert len(errors) == 1
    assert errors[0].startswith("Dockerfile:1:")
    assert "python:3.14" in errors[0]
    assert pins().image_errors("FROM scratch\n", "Dockerfile") == []
    assert pins().image_errors("FROM $IMAGE\n", "Dockerfile") == []


def test_pinned_from_passes() -> None:
    text = f"FROM docker.io/library/python:3.14.8-alpine3.24@sha256:{DIGEST} AS builder\n"
    assert pins().image_errors(text, "Dockerfile") == []


def test_composite_action_is_scanned(tmp_path: Path) -> None:
    (tmp_path / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "1.99.1"\n')
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text("name: CI\non: push\njobs: {}\n")
    action = tmp_path / ".github" / "actions" / "build"
    action.mkdir(parents=True)
    (action / "action.yml").write_text(
        "name: build\nruns:\n  using: composite\n  steps:\n    - uses: actions/checkout@v4\n"
    )
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "maturin-requirements.txt").write_text(
        f"maturin==1.15.0 \\\n    --hash=sha256:{DIGEST}\n"
    )
    errors = pins().check_repository(tmp_path)
    assert len(errors) == 1
    assert ".github/actions/build/action.yml" in errors[0]
    assert "actions/checkout@v4" in errors[0]


def test_bare_name_is_implicit_latest() -> None:
    assert_fails(
        "name: CI\non: push\njobs:\n  t:\n    container: alpine\n    steps: []\n",
        "implicit :latest",
    )
    assert_fails(
        "name: CI\non: push\njobs:\n  t:\n    services:\n      a:\n        image: alpine\n"
        "    steps: []\n",
        "implicit :latest",
    )
    errors = pins().image_errors("docker pull alpine\n", "ci.sh")
    assert len(errors) == 1
    assert "implicit :latest" in errors[0]
    errors = pins().image_errors("FROM alpine\n", "Dockerfile")
    assert len(errors) == 1
    assert "alpine" in errors[0]
    assert "implicit :latest" in errors[0]


def test_untagged_registry_image_fails() -> None:
    assert_fails(
        "name: CI\non: push\njobs:\n  t:\n    container: docker.io/library/alpine\n    steps: []\n",
        "implicit :latest",
    )
    errors = pins().image_errors("FROM docker.io/library/alpine AS x\n", "Dockerfile")
    assert len(errors) == 1
    assert "docker.io/library/alpine" in errors[0]
    assert "implicit :latest" in errors[0]
    errors = pins().image_errors("X=quay.io/pypa/manylinux2014_x86_64\n", "Makefile")
    assert len(errors) == 1
    assert "implicit :latest" in errors[0]


def test_container_image_key_fails() -> None:
    text = (
        "name: CI\non: push\njobs:\n  t:\n    container:\n      image: python:3.14\n    steps: []\n"
    )
    assert_fails(text, "python:3.14")


def test_pull_podman_and_from_flag() -> None:
    errors = pins().image_errors("podman pull python:3.14\n", "ci.sh")
    assert len(errors) == 1
    assert "python:3.14" in errors[0]
    errors = pins().image_errors("docker pull docker.io/library/alpine\n", "ci.sh")
    assert len(errors) == 1
    assert "implicit :latest" in errors[0]
    errors = pins().image_errors("docker build --from python:3.14 .\n", "ci.sh")
    assert len(errors) == 1
    assert "python:3.14" in errors[0]
    assert pins().image_errors("COPY --from=builder /a /b\n", "Dockerfile") == []
    assert pins().image_errors("kubectl create secret --from-literal=password=x\n", "ci.sh") == []


def test_make_and_arg_image_defaults_fail() -> None:
    errors = pins().image_errors("IMG := python:3.14\n", "Makefile")
    assert len(errors) == 1
    assert "python:3.14" in errors[0]
    errors = pins().image_errors("ARG PYTHON=python:3.14\n", "Dockerfile")
    assert len(errors) == 1
    assert "python:3.14" in errors[0]
    assert pins().image_errors("ARG SCHEDR=schedr:0.1.0\n", "Dockerfile") == []
    assert pins().image_errors("IMAGE ?= schedr:local\n", "Makefile") == []


def test_labels_urls_and_python_imports_are_not_images() -> None:
    text = (
        "postgres://postgres:schedr@127.0.0.1:5432/postgres\n"
        "helm template t --api-versions networking.istio.io/v1\n"
        "from datetime import timedelta\n"
        "kubectl label app.kubernetes.io/instance=schedr\n"
    )
    assert pins().image_errors(text, "ci.sh") == []


def test_rust_toolchain_needs_a_toolchain() -> None:
    text = (
        "name: CI\non: push\njobs:\n  rust:\n    steps:\n"
        f"      - uses: dtolnay/rust-toolchain@{PIN}\n"
        "        with:\n"
        "          components: rustfmt\n"
    )
    assert_fails(text, "needs `toolchain:`")


def test_shell_script_image_is_scanned(tmp_path: Path) -> None:
    (tmp_path / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "1.99.1"\n')
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text("name: CI\non: push\njobs: {}\n")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "maturin-requirements.txt").write_text(
        f"maturin==1.15.0 \\\n    --hash=sha256:{DIGEST}\n"
    )
    (scripts / "bad.sh").write_text("docker pull alpine\n")
    errors = pins().check_repository(tmp_path)
    assert len(errors) == 1
    assert "scripts/bad.sh" in errors[0]
    assert "alpine" in errors[0]


def test_this_repository_is_pinned() -> None:
    assert pins().check_repository(ROOT) == []
