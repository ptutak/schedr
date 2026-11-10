"""Plays the greenhouse for the showcase's DAGs: its devices, its operator and the pager.

Against a standalone with these DAGs (`make standalone DAGS=examples/showcase/dags`), in a second
terminal:

    make showcase                              # the whole scenario, below
    python examples/showcase/simulate.py receive   # only the pager's receiver
    python examples/showcase/simulate.py devices --rounds 3

`demo` (what `make showcase` runs) starts the receiver, then writes three rounds of readings, a
round every 20 seconds: four devices in two zones, a corrupt file in the second round, and two
devices running hot in the third. Once `readings_ingest` has posted their alarms it acknowledges
`dev-a1` (`schedr events post ack`), so its alarm goes on to `device_maintenance`, and leaves
`dev-b2` unanswered, so its run pages after two minutes. The receiver prints what the pager gets.
`make showcase` unpauses the four DAGs first; run by hand, unpause them with `schedr dags unpause`.
"""

import argparse
import json
import random
import subprocess
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import override

INBOX = Path("/tmp/schedr-greenhouse/inbox")
"""The DAGs' inbox (`greenhouse.INBOX`)."""
DEVICES = ("dev-a1", "dev-a2", "dev-b1", "dev-b2")
HOT = ("dev-a1", "dev-b2")
"""The devices that run hot in the last round: one acknowledged, one not."""
CORRUPT_ROUND = 2
"""The round with a corrupt file in it."""
PORT = 9099
"""The receiver's port: `pager`'s URL, allowed by examples/standalone.toml."""


class _Pager(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("content-length", "0")))
        try:
            shown = json.dumps(json.loads(body), indent=2)
        except ValueError:
            shown = body.decode(errors="replace")
        print(f"\n--- pager {self.path} ---\n{shown}", flush=True)
        self.send_response(204)
        self.end_headers()

    @override
    def log_message(self, format: str, *args: object) -> None:
        del format, args


def receive() -> ThreadingHTTPServer:
    """Starts the pager's receiver in a thread of its own."""
    server = ThreadingHTTPServer(("127.0.0.1", PORT), _Pager)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"pager receiver on http://127.0.0.1:{PORT}/", flush=True)
    return server


def write_round(number: int, *, corrupt: bool, hot: tuple[str, ...]) -> None:
    """One file per zone, named by the time so later files sort after earlier ones."""
    INBOX.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)
    for zone in ("a", "b"):
        lines: list[str] = []
        for device in (d for d in DEVICES if d.startswith(f"dev-{zone}")):
            celsius = 36.0 + random.uniform(0, 3) if device in hot else random.uniform(20, 26)
            lines.append(
                json.dumps(
                    {
                        "device": device,
                        "at": now.isoformat(),
                        "celsius": round(celsius, 1),
                        "humidity": round(random.uniform(40, 70), 1),
                    }
                )
            )
        name = f"{now:%Y%m%dT%H%M%S%f}-zone_{zone}.jsonl"
        # Written aside and renamed: the trigger never sees half a file.
        part = INBOX / f".{name}.part"
        part.write_text("\n".join(lines) + "\n")
        part.rename(INBOX / name)
    if corrupt:
        (INBOX / f"{now:%Y%m%dT%H%M%S%f}-zone_c.jsonl").write_text('{"device": "dev-c1", oops\n')
    said = f", {' and '.join(hot)} hot" if hot else ""
    print(f"round {number}: wrote readings{', and a corrupt file' if corrupt else ''}{said}")


def devices(rounds: int, every: float) -> None:
    for number in range(1, rounds + 1):
        last = number == rounds
        write_round(number, corrupt=number == CORRUPT_ROUND, hot=HOT if last else ())
        if not last:
            time.sleep(every)


def ack(device: str, schedr: str) -> None:
    """The operator acknowledges `device`'s alarm."""
    data = json.dumps({"by": "ana", "action": "recalibrate"})
    subprocess.run([schedr, "events", "post", "ack", "--key", device, "--data", data], check=True)
    print(f"acknowledged {device}")


def demo(schedr: str) -> None:
    receive()
    devices(rounds=3, every=20)
    # The batch window (30 s) closes, the run detects and posts the alarms.
    print("waiting a minute for readings_ingest to raise the alarms ...", flush=True)
    time.sleep(60)
    ack(HOT[0], schedr)
    print(f"{HOT[1]} left unanswered: its run pages in two minutes. Ctrl-C to stop.", flush=True)
    while True:
        time.sleep(3600)


class _Args(argparse.Namespace):
    """The command line, typed."""

    command: str = ""
    schedr: str = "schedr"
    rounds: int = 3
    every: float = 20.0
    device: str = ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Plays the greenhouse for the showcase's DAGs.")
    parser.add_argument("--schedr", default="schedr", help="the schedr CLI (default: on PATH)")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo", help="the whole scenario")
    commands.add_parser("receive", help="only the pager's receiver")
    rounds = commands.add_parser("devices", help="only the devices")
    rounds.add_argument("--rounds", type=int, default=3)
    rounds.add_argument("--every", type=float, default=20.0)
    acking = commands.add_parser("ack", help="acknowledge a device's alarm")
    acking.add_argument("device")
    args = parser.parse_args(namespace=_Args())
    try:
        match args.command:
            case "demo":
                demo(args.schedr)
            case "receive":
                receive()
                while True:
                    time.sleep(3600)
            case "devices":
                devices(args.rounds, args.every)
            case "ack":
                ack(args.device, args.schedr)
            case other:
                parser.error(f"no command {other!r}")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
