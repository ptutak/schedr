"""The local stand-in for the master Service in `make standalone-separate-ui`."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import socket
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def proxy() -> ModuleType:
    """Load scripts/master_proxy.py."""
    path = ROOT / "scripts" / "master_proxy.py"
    spec = importlib.util.spec_from_file_location("master_proxy", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def free_port() -> int:
    """A port nothing listens on (bound, then released)."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
    return port


def test_a_master_is_its_grpc_and_ops_ports() -> None:
    assert proxy().parse_master("8072:8090") == proxy().Master(8072, 8090)


@pytest.mark.parametrize("text", ["8072", "8072:", ":8090", "a:b", "8072-8090"])
def test_a_master_not_written_as_two_ports_is_refused(text: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="GRPC_PORT:OPS_PORT"):
        proxy().parse_master(text)


def test_a_master_not_answering_isnt_ready_and_none_leads() -> None:
    port = free_port()
    assert not asyncio.run(proxy().is_ready(port, timeout=0.5))
    assert asyncio.run(proxy().leader([proxy().Master(free_port(), port)])) is None


def test_connections_follow_readiness_from_one_master_to_the_other() -> None:
    # The check mode's two fake masters: the ready one, the other after a takeover, none.
    assert asyncio.run(proxy().check()) == 0


def test_serving_needs_a_port_and_a_master() -> None:
    with pytest.raises(SystemExit):
        proxy().main(["--listen", "8070"])
