#!/usr/bin/env python3
"""Fail when CI's supply-chain pins are missing.

Checks, against PROJECT.md (CI supply-chain pins):

- every ``uses:`` is a 40-character commit SHA, a local ``./...`` action or
  reusable workflow, or ``docker://image@sha256:<64 hex>``;
- every ``toolchain:`` and maturin-action ``rust-toolchain:`` equals the channel
  in ``rust-toolchain.toml`` (an expression does not);
- ``dtolnay/rust-toolchain`` sets ``toolchain:`` to the channel in the file;
  ``taiki-e/install-action``'s ``tool:`` is ``name@version``; ``astral-sh/setup-uv``
  and ``azure/setup-helm`` set ``version:`` to ``x.y.z``;
- every image used by a workflow (including ``container:`` and ``services``),
  ``scripts/*.sh``, the Makefile, ``images/**/Dockerfile`` or
  ``examples/kubernetes/Dockerfile`` is pinned to ``@sha256:<64 hex>`` (no tag is
  implicit ``:latest``);
- ``scripts/maturin-requirements.txt`` carries a sha256 hash for every requirement.

PyYAML is a direct dev dependency. ``make pin-check`` runs it with ``uv run --no-sync``.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")
VERSION = re.compile(r"^v?[0-9]+\.[0-9]+\.[0-9]+$")
# Dotted registry, a path, and either :tag or @sha256:<64>. Found inside a line, so
# `IMAGE=quay.io/org/name:tag` in a shell script is not skipped.
_IMAGE_FIND = re.compile(
    r"(?:docker://)?"
    r"(?P<ref>[a-z0-9][a-z0-9.-]*\.[a-z0-9.-]+(?::[0-9]+)?"
    r"/[a-z0-9][a-z0-9._/-]*"
    r"(?P<tag>:[A-Za-z0-9_][A-Za-z0-9_.-]*)?"
    r"(?:@sha256:(?P<digest>[0-9a-f]{64}))?)"
)
_IMAGE = re.compile(rf"^{_IMAGE_FIND.pattern}$")
# name:tag, optionally already digest-pinned. Used for short names on run/FROM lines.
_SHORT = re.compile(
    r"^(?P<ref>[a-z0-9][a-z0-9._-]*:[A-Za-z0-9_][A-Za-z0-9_.-]*)"
    r"(?:@sha256:(?P<digest>[0-9a-f]{64}))?$"
)
# A Dockerfile instruction, not Python's `from datetime import timedelta`.
_FROM = re.compile(
    r"^\s*FROM\s+(?:--platform=\S+\s+)?(\S+)(?:\s+[Aa][Ss]\s+\S+)?\s*$",
    re.IGNORECASE,
)
_TOKEN = re.compile(r"[^\s\"'`]+")
_HASH = re.compile(r"--hash=sha256:[0-9a-f]{64}\b")
_REQUIREMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*(\[[^\]]*\])?==")
_TOOLCHAIN_KEYS = frozenset({"toolchain", "rust-toolchain"})
_TOOL_ACTIONS = {
    "taiki-e/install-action": "tool",
    "astral-sh/setup-uv": "version",
    "azure/setup-helm": "version",
}

_PIN_USES = (
    "pin it as `owner/repo@<sha> # <version>`, a local `./...` action, "
    "or `docker://image@sha256:<64 hex>` (PROJECT.md, CI supply-chain pins)"
)


def channel(root: Path) -> str:
    """Return the Rust channel pinned in rust-toolchain.toml."""
    data = tomllib.loads((root / "rust-toolchain.toml").read_text())
    return str(data["toolchain"]["channel"])


def _display(path: Path, root: Path) -> str:
    """Path relative to the repository when it lives there."""
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _strip_comment(line: str) -> str:
    """Drop a ``#`` comment that is not inside quotes."""
    out: list[str] = []
    quote = ""
    for char in line:
        if quote:
            out.append(char)
            if char == quote:
                quote = ""
            continue
        if char in {"'", '"'}:
            quote = char
            out.append(char)
            continue
        if char == "#":
            break
        out.append(char)
    return "".join(out)


def _tokens(line: str) -> list[str]:
    """Whitespace-separated tokens, with comments already removed."""
    tokens: list[str] = []
    for raw in _TOKEN.findall(line):
        token = raw.strip("(),")
        if token.startswith(("https://", "http://", "postgres://")):
            continue
        tokens.append(token)
    return tokens


def _pin_error(image: str) -> str:
    """The error for an image that is missing ``@sha256:<64 hex>``."""
    name = image.split("@", 1)[0]
    path = name.split("/", 1)[-1]
    if ":" not in path:
        why = "has no tag or digest (implicit :latest) and is not pinned to a sha256 digest"
    else:
        why = "is not pinned to a sha256 digest"
    return (
        f"image `{image}` {why}; "
        "pin it as `name@sha256:<64 hex>` (PROJECT.md, CI supply-chain pins)"
    )


def _explicit_image_error(image: str) -> str | None:
    """An error for an image named by ``pull``, ``FROM``, ``--from`` or ``run``.

    A tagged registry image is reported by the line scan, so this stays quiet for it.
    An untagged registry image (``docker.io/library/alpine``, implicit ``:latest``) is
    only an image in one of these places.
    """
    image = image.strip().strip("'\"")
    matched = _IMAGE.fullmatch(image)
    if matched:
        if matched.group("digest") or matched.group("tag"):
            return None
        return _pin_error(image)
    return _short_or_bare(image)


def _assignment_image_error(value: str) -> str | None:
    """An error when ``NAME=value`` assigns an unpinned image.

    A bare word (``PASSWORD=schedr``) is not an image. ``python:3.14`` and an untagged
    ``quay.io/org/name`` are. A tagged registry image is reported by the line scan.
    """
    value = value.strip().strip("'\"")
    if value.split(":", 1)[0] == "schedr":
        return None
    matched = _IMAGE.fullmatch(value)
    if matched:
        if matched.group("digest") or matched.group("tag"):
            return None
        return _pin_error(value)
    if _SHORT.fullmatch(value):
        return _pin_error(value)
    return None


def _url_prefix(code: str, start: int) -> bool:
    """True when the match sits in an http(s) or postgres URL, not an image pull."""
    return code[:start].endswith(("https://", "http://", "postgres://"))


# `NAME := value`, `NAME=value`, `ARG NAME=value`. A dotted registry image on the
# same line is reported by the registry scan, so this only adds short `name:tag`.
_ASSIGN = re.compile(
    r"(?:^|[\s])(?:ARG\s+|export\s+)?[A-Za-z_][A-Za-z0-9_]*\s*(?:\?=|:=|\+=|=)\s*(\S+)"
)
_PULL = re.compile(r"\b(?:docker|podman)\s+pull\s+(\S+)")
_FROM_FLAG = re.compile(r"--from(?:=|\s+)(\S+)")
_RUN_WITH_VALUE = frozenset(
    {
        "-v",
        "-e",
        "-w",
        "-u",
        "-p",
        "-n",
        "--name",
        "--entrypoint",
        "--network",
        "--user",
        "--workdir",
        "--volume",
        "--env",
        "--mount",
        "--label",
        "-l",
        "--add-host",
        "--hostname",
        "--memory",
        "--cpus",
        "--pull",
    }
)


def _docker_run_image(code: str) -> str | None:
    """The image argument of a ``docker run`` / ``podman run``, if the line has one."""
    matched = re.search(r"\b(?:docker|podman)\s+run\b(.*)$", code)
    if matched is None:
        return None
    tokens = _tokens(matched.group(1))
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in {"--rm", "-d", "-i", "-t", "-it", "-ti", "--init", "--privileged"}:
            index += 1
            continue
        if token in _RUN_WITH_VALUE:
            index += 2
            continue
        if token.startswith("-"):
            index += 1
            continue
        return token
    return None


def _report(errors: list[str], display: str, number: int, problem: str | None) -> None:
    """Append one formatted error."""
    if problem:
        errors.append(f"{display}:{number}: {problem}")


def _short_or_bare(image: str) -> str | None:
    """An error for a short image (``name:tag`` or a bare name). Registry refs are separate."""
    image = image.strip().strip("'\"")
    if _IMAGE.fullmatch(image) or DIGEST.search(image):
        return None
    if image.startswith("$") or image.startswith("${") or image == "scratch":
        return None
    # `schedr:0.1.0` is the image this repository builds, not a registry pull.
    if image.split(":", 1)[0] == "schedr":
        return None
    if _SHORT.fullmatch(image) or re.fullmatch(r"[a-z0-9][a-z0-9._-]*", image):
        return _pin_error(image)
    return None


def image_errors(text: str, display: str, first_line: int = 1) -> list[str]:
    """Return one error per unpinned image reference in ``text``."""
    errors: list[str] = []
    for offset, line in enumerate(text.splitlines() or [""]):
        number = first_line + offset
        code = _strip_comment(line)
        for found in _IMAGE_FIND.finditer(code):
            if _url_prefix(code, found.start()):
                continue
            # No tag: `foo.bar/baz` is a label, an API group or a URL path, not a pull.
            # Untagged registry images are checked where a line names one.
            if found.group("digest") or not found.group("tag"):
                continue
            _report(errors, display, number, _pin_error(found.group(0)))
        run_image = _docker_run_image(code)
        if run_image is not None:
            _report(errors, display, number, _explicit_image_error(run_image))
        for found in _PULL.finditer(code):
            _report(errors, display, number, _explicit_image_error(found.group(1)))
        for found in _FROM_FLAG.finditer(code):
            value = found.group(1)
            # `COPY --from=builder` names a stage. An image has a tag, a registry, or a digest.
            if "/" not in value and ":" not in value and "@" not in value:
                continue
            _report(errors, display, number, _explicit_image_error(value))
        for found in _ASSIGN.finditer(code):
            _report(errors, display, number, _assignment_image_error(found.group(1)))
        from_match = _FROM.match(code)
        if from_match:
            _report(errors, display, number, _explicit_image_error(from_match.group(1)))
    return errors


def _scalar(node: yaml.Node) -> str | None:
    """The string value of a scalar node, or None."""
    if isinstance(node, yaml.ScalarNode):
        return node.value
    return None


def _line(node: yaml.Node) -> int:
    """1-based line number of a node."""
    return node.start_mark.line + 1


def _check_uses(ref: str, line: int, display: str) -> str | None:
    """An error when ``ref`` is not an accepted ``uses:`` pin."""
    ref = ref.strip()
    if ref.startswith("./"):
        return None
    if re.fullmatch(r"docker://\S+@sha256:[0-9a-f]{64}", ref):
        return None
    revision = ref.rsplit("@", 1)[-1] if "@" in ref else ""
    if SHA.fullmatch(revision):
        return None
    return (
        f"{display}:{line}: `uses: {ref}` is not pinned to a 40-character commit SHA; {_PIN_USES}"
    )


def _check_toolchain(
    key: str, value: str, line: int, display: str, rust_channel: str
) -> str | None:
    """An error when a toolchain input is not the pinned channel."""
    if value.strip() != rust_channel or "${{" in value:
        return (
            f"{display}:{line}: `{key}: {value}` does not match "
            f"rust-toolchain.toml channel `{rust_channel}`"
        )
    return None


def _action_name(uses: str) -> str:
    """``owner/repo`` from a ``uses:`` value, without the ref."""
    if uses.startswith("./") or uses.startswith("docker://"):
        return uses
    return uses.split("@", 1)[0]


def _mapping_scalars(node: yaml.Node) -> dict[str, tuple[str, int]]:
    """Scalar inputs of a mapping node, keyed by name, with line numbers."""
    found: dict[str, tuple[str, int]] = {}
    if not isinstance(node, yaml.MappingNode):
        return found
    for key_node, value_node in node.value:
        key = _scalar(key_node)
        value = _scalar(value_node)
        if key is None or value is None:
            continue
        found[key] = (value, _line(value_node))
    return found


def _check_tool_inputs(uses: str, with_node: yaml.Node, display: str) -> list[str]:
    """Errors for unpinned tool versions on the actions that take one."""
    name = _action_name(uses.strip())
    if name not in _TOOL_ACTIONS:
        return []
    errors: list[str] = []
    inputs = _mapping_scalars(with_node)
    if name == "taiki-e/install-action":
        if "tool" not in inputs:
            errors.append(
                f"{display}:{_line(with_node)}: `taiki-e/install-action` needs "
                "`tool:` set to `name@version` (PROJECT.md, CI supply-chain pins)"
            )
            return errors
        value, line = inputs["tool"]
        parts = [part.strip() for part in value.split(",") if part.strip()]
        if not parts:
            errors.append(f"{display}:{line}: `tool:` is empty; pin each tool as `name@version`")
        for part in parts:
            tool, separator, version = part.partition("@")
            if (
                not separator
                or not tool
                or not version
                or "${{" in part
                or version in {"latest", "stable"}
                or not re.search(r"\d", version)
            ):
                errors.append(
                    f"{display}:{line}: `tool: {part}` has no @version; "
                    "pin it as `name@version` (taiki-e/install-action)"
                )
        return errors
    value_line = inputs.get("version")
    if value_line is None:
        errors.append(
            f"{display}:{_line(with_node)}: `{name}` needs `version:` set to an "
            "exact `x.y.z` (PROJECT.md, CI supply-chain pins)"
        )
        return errors
    value, line = value_line
    if "${{" in value or VERSION.fullmatch(value.strip()) is None:
        errors.append(
            f"{display}:{line}: `{name}` `version: {value}` is not an exact "
            "`x.y.z` (PROJECT.md, CI supply-chain pins)"
        )
    return errors


def _walk(node: yaml.Node, display: str, rust_channel: str, errors: list[str]) -> None:
    """Walk a YAML node and record pin errors."""
    if isinstance(node, yaml.MappingNode):
        uses_ref: str | None = None
        with_node: yaml.Node | None = None
        for key_node, value_node in node.value:
            key = _scalar(key_node)
            if key == "uses":
                ref = _scalar(value_node)
                if ref is None:
                    errors.append(
                        f"{display}:{_line(value_node)}: `uses:` must be a string pin, "
                        f"not a {_kind(value_node)}; {_PIN_USES}"
                    )
                else:
                    uses_ref = ref
                    problem = _check_uses(ref, _line(value_node), display)
                    if problem:
                        errors.append(problem)
            elif key in _TOOLCHAIN_KEYS:
                value = _scalar(value_node)
                if value is None:
                    errors.append(
                        f"{display}:{_line(value_node)}: `{key}:` must be the channel "
                        f"`{rust_channel}` from rust-toolchain.toml, not a {_kind(value_node)}"
                    )
                else:
                    problem = _check_toolchain(key, value, _line(value_node), display, rust_channel)
                    if problem:
                        errors.append(problem)
            elif key == "with":
                with_node = value_node
            elif key == "image" or (key == "container" and isinstance(value_node, yaml.ScalarNode)):
                value = _scalar(value_node)
                if value is None or DIGEST.search(value.strip()) is None:
                    shown = value if value is not None else _kind(value_node)
                    errors.append(f"{display}:{_line(value_node)}: {_pin_error(str(shown))}")
            if isinstance(value_node, yaml.ScalarNode) and key not in {"image", "container"}:
                errors.extend(image_errors(value_node.value, display, _line(value_node)))
            else:
                _walk(value_node, display, rust_channel, errors)
        if uses_ref is not None:
            action = _action_name(uses_ref.strip())
            if action == "dtolnay/rust-toolchain":
                inputs = _mapping_scalars(with_node) if with_node is not None else {}
                if "toolchain" not in inputs:
                    where = with_node if with_node is not None else node
                    errors.append(
                        f"{display}:{_line(where)}: `dtolnay/rust-toolchain` needs "
                        f"`toolchain:` set to `{rust_channel}` from rust-toolchain.toml "
                        "(PROJECT.md, CI supply-chain pins)"
                    )
            if action in _TOOL_ACTIONS:
                if with_node is None:
                    errors.append(
                        f"{display}:{_line(node)}: `{action}` needs "
                        "its version input set (PROJECT.md, CI supply-chain pins)"
                    )
                else:
                    errors.extend(_check_tool_inputs(uses_ref, with_node, display))
        return
    if isinstance(node, yaml.SequenceNode):
        for item in node.value:
            _walk(item, display, rust_channel, errors)


def _kind(node: yaml.Node) -> str:
    """A short name for a YAML node, for an error."""
    if isinstance(node, yaml.MappingNode):
        return "mapping"
    if isinstance(node, yaml.SequenceNode):
        return "list"
    return "value"


def check_yaml(text: str, display: str, rust_channel: str) -> list[str]:
    """Return pin errors for one YAML document."""
    try:
        loader = yaml.SafeLoader(text)
        try:
            node = loader.get_single_node()
        finally:
            loader.dispose()
    except yaml.YAMLError as exc:
        return [f"{display}: YAML could not be parsed: {exc}"]
    if node is None:
        return []
    errors: list[str] = []
    _walk(node, display, rust_channel, errors)
    return errors


def check_requirements(text: str, display: str) -> list[str]:
    """Return errors when a requirements file is missing hashes."""
    logical: list[tuple[int, str]] = []
    buffer = ""
    start = 1
    lines = text.splitlines()
    for number, line in enumerate(lines, 1):
        code = _strip_comment(line).strip()
        if not code:
            continue
        if not buffer:
            start = number
        if code.endswith("\\"):
            buffer += code[:-1].strip() + " "
            continue
        buffer += code
        logical.append((start, buffer.strip()))
        buffer = ""
    if buffer.strip():
        logical.append((start, buffer.strip()))

    blocks: list[tuple[int, str]] = []
    current_line = 0
    current = ""
    for number, line in logical:
        if line.startswith("--hash"):
            if not current:
                return [f"{display}:{number}: `--hash` is not attached to a requirement"]
            current += " " + line
            continue
        if _REQUIREMENT.match(line):
            if current:
                blocks.append((current_line, current))
            current_line = number
            current = line
            continue
        if line.startswith("-"):
            if current:
                current += " " + line
            continue
        return [f"{display}:{number}: unexpected requirements line `{line}`"]
    if current:
        blocks.append((current_line, current))
    if not blocks:
        return [f"{display}: no requirements found; expected maturin pinned with hashes"]
    errors: list[str] = []
    for number, line in blocks:
        hashes = _HASH.findall(line)
        hash_marks = line.count("--hash=")
        if not hashes or hash_marks != len(hashes):
            name = line.split()[0]
            errors.append(
                f"{display}:{number}: requirement `{name}` has no `--hash=sha256:<64 hex>` "
                "(PROJECT.md, CI supply-chain pins)"
            )
    return errors


def _workflow_files(root: Path) -> list[Path]:
    """Workflow files and composite actions."""
    workflows = root / ".github" / "workflows"
    actions = root / ".github" / "actions"
    found: list[Path] = []
    if workflows.is_dir():
        found.extend(sorted(workflows.glob("*.yml")))
        found.extend(sorted(workflows.glob("*.yaml")))
    if actions.is_dir():
        found.extend(sorted(actions.glob("**/action.yml")))
        found.extend(sorted(actions.glob("**/action.yaml")))
    return found


def _image_files(root: Path) -> list[Path]:
    """Shell, Make and Dockerfiles whose image references are not in workflow YAML."""
    found: list[Path] = []
    scripts = root / "scripts"
    if scripts.is_dir():
        found.extend(sorted(path for path in scripts.glob("*.sh") if path.is_file()))
    makefile = root / "Makefile"
    if makefile.is_file():
        found.append(makefile)
    images = root / "images"
    if images.is_dir():
        found.extend(sorted(images.glob("**/Dockerfile")))
    example = root / "examples" / "kubernetes" / "Dockerfile"
    if example.is_file():
        found.append(example)
    return found


def check_repository(root: Path) -> list[str]:
    """Return every pin error under ``root``."""
    errors: list[str] = []
    try:
        rust_channel = channel(root)
    except (OSError, tomllib.TOMLDecodeError, KeyError) as exc:
        return [f"rust-toolchain.toml: cannot read the channel ({exc})"]
    for path in _workflow_files(root):
        display = _display(path, root)
        errors.extend(check_yaml(path.read_text(), display, rust_channel))
    for path in _image_files(root):
        display = _display(path, root)
        errors.extend(image_errors(path.read_text(), display))
    requirements = root / "scripts" / "maturin-requirements.txt"
    display = _display(requirements, root)
    if not requirements.is_file():
        errors.append(
            f"{display}: missing; maturin is installed with "
            "`pip install --require-hashes` from this file"
        )
    else:
        errors.extend(check_requirements(requirements.read_text(), display))
    return errors


def main(argv: list[str]) -> int:
    """Check the repository, or the YAML files named on the command line."""
    if argv:
        root = ROOT
        try:
            rust_channel = channel(root)
        except (OSError, tomllib.TOMLDecodeError, KeyError) as exc:
            print(f"error: rust-toolchain.toml: cannot read the channel ({exc})", file=sys.stderr)
            return 1
        errors: list[str] = []
        for arg in argv:
            path = Path(arg)
            errors.extend(check_yaml(path.read_text(), str(path), rust_channel))
    else:
        errors = check_repository(ROOT)
    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
