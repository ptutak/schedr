#!/usr/bin/env python3
"""A local stand-in for the chart's master Service, for `make standalone-separate-ui`.

Workers name one `[worker] master_url`. In Kubernetes it is the master Service, which routes
only to the master whose `/readyz` answers (the leader: a standby or paused master answers
503), so a takeover reaches the workers by itself. On one machine nothing does that: this proxy
listens on one port and sends each new connection to whichever master's ops port answers
`/readyz` with 200. A connection already open stays with its master; when that master stops
or loses the lead it ends the stream, the worker reconnects, and the new connection goes to the
new leader.

    master_proxy.py --listen 8070 --master 8072:8090 --master 8071:8091
    master_proxy.py --check     # proves the routing on throwaway local ports, then exits

Only the standard library; plain TCP (gRPC over HTTP/2 without TLS, `[tls] mode = "mesh"`).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

HOST = "127.0.0.1"
# How long a readiness check may take before that master counts as not ready.
READY_TIMEOUT = 1.0
CHUNK = 64 * 1024

Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


@dataclass(frozen=True)
class Master:
    """A master's gRPC port, where connections go, and its ops port, where `/readyz` is."""

    grpc: int
    ops: int


def parse_master(text: str) -> Master:
    """`GRPC:OPS`, e.g. `8072:8090`."""
    grpc, sep, ops = text.partition(":")
    if not sep or not grpc.isdigit() or not ops.isdigit():
        raise argparse.ArgumentTypeError(f"`{text}`: expected GRPC_PORT:OPS_PORT, e.g. 8072:8090")
    return Master(int(grpc), int(ops))


async def is_ready(ops: int, timeout: float = READY_TIMEOUT) -> bool:
    """Whether `GET /readyz` on the ops port answers 200 within `timeout`."""
    try:
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_connection(HOST, ops)
            try:
                writer.write(
                    b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                await writer.drain()
                status = await reader.readline()
            finally:
                writer.close()
                with contextlib.suppress(OSError):
                    await writer.wait_closed()
    except OSError, TimeoutError:
        return False
    # `HTTP/1.1 200 OK`: the status code is the second word.
    _, code, *_ = [*status.split(), b"", b""]
    return code == b"200"


async def leader(masters: list[Master]) -> Master | None:
    """The first master whose `/readyz` answers 200, all checked at once; `None` if none."""
    ready = await asyncio.gather(*(is_ready(m.ops) for m in masters))
    return next((m for m, ok in zip(masters, ready, strict=True) if ok), None)


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Copies `reader` to `writer` until either side ends, then closes `writer`."""
    try:
        while data := await reader.read(CHUNK):
            writer.write(data)
            await writer.drain()
    except OSError:
        pass
    finally:
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()


def handler(masters: list[Master], log: bool = True) -> Handler:
    """The connection callback: each connection to the leader, or closed when none leads."""
    said: list[int | None] = [None]

    async def handle(client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
        chosen = await leader(masters)
        if log and (chosen.grpc if chosen else None) != said[0]:
            said[0] = chosen.grpc if chosen else None
            where = f"the master on {chosen.grpc}" if chosen else "none (no master ready)"
            print(f"master_proxy: connections go to {where}", file=sys.stderr, flush=True)
        if chosen is None:
            # The worker reconnects with its backoff, as from a Service with no ready pod.
            client_w.close()
            return
        try:
            master_r, master_w = await asyncio.open_connection(HOST, chosen.grpc)
        except OSError:
            client_w.close()
            return
        await asyncio.gather(pipe(client_r, master_w), pipe(master_r, client_w))

    return handle


async def serve(port: int, masters: list[Master]) -> None:
    """Listens on `port` (loopback) until cancelled."""
    server = await asyncio.start_server(handler(masters), HOST, port)
    async with server:
        await server.serve_forever()


async def check() -> int:
    """Prove the routing on two fake masters, one ready at a time; 0 when it holds.

    Each fake has an echo port and a `/readyz`: a connection reaches the ready one, then the
    other once readiness moves, and none while neither is ready.
    """
    ready = {"a": True, "b": False}

    def fake(name: str) -> tuple[Handler, Handler]:
        async def grpc(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            w.write(name.encode())
            await w.drain()
            w.close()

        async def ops(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            await r.readline()
            code = b"200 OK" if ready[name] else b"503 Service Unavailable"
            w.write(b"HTTP/1.1 " + code + b"\r\nContent-Length: 0\r\n\r\n")
            await w.drain()
            w.close()

        return grpc, ops

    servers = []
    masters = []
    for name in ("a", "b"):
        grpc, ops = fake(name)
        g = await asyncio.start_server(grpc, HOST, 0)
        o = await asyncio.start_server(ops, HOST, 0)
        servers += [g, o]
        masters.append(Master(g.sockets[0].getsockname()[1], o.sockets[0].getsockname()[1]))
    proxy = await asyncio.start_server(handler(masters, log=False), HOST, 0)
    servers.append(proxy)
    port = proxy.sockets[0].getsockname()[1]

    async def through() -> bytes:
        r, w = await asyncio.open_connection(HOST, port)
        data = await r.read()
        w.close()
        return data

    first = await through()
    ready.update(a=False, b=True)
    second = await through()
    ready.update(b=False)
    third = await through()
    for s in servers:
        s.close()
    results = {
        "to the ready one": first == b"a",
        "after a takeover": second == b"b",
        "none while none is ready": third == b"",
    }
    for what, ok in results.items():
        print(f"{'ok  ' if ok else 'FAIL'} {what}")
    return 0 if all(results.values()) else 1


def main(argv: list[str] | None = None) -> int:
    """Serve, or with `--check` prove the routing; the exit code."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--listen", type=int, help="the port workers connect to (loopback)")
    parser.add_argument(
        "--master",
        type=parse_master,
        action="append",
        default=[],
        help="GRPC_PORT:OPS_PORT of a master; repeat for each",
    )
    parser.add_argument("--check", action="store_true", help="prove the routing and exit")
    args = parser.parse_args(argv)
    if args.check:
        return asyncio.run(check())
    if args.listen is None or not args.master:
        parser.error("--listen and at least one --master are needed (or --check)")
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve(args.listen, args.master))
    return 0


if __name__ == "__main__":
    sys.exit(main())
