#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["bcrypt==5.0.0"]
# ///
"""Write the users file of schedr's `basic` sign-in, and a Kubernetes Secret holding it.

The file is `[webserver.auth]` / `[ui.auth] users_file`'s `name:bcrypt-hash` lines; the Secret
is the chart's `auth.usersSecret` (key `users`).

Passwords are asked for, twice each (never arguments: they would show in `ps` and the shell's
history), or read as `name:password` lines from standard input with `--stdin` (a password
manager's export, say). The plain passwords are written nowhere. Both files are written whole
from the users given: to change one password, give them all again.

Usage:
    scripts/users-file.py --out ~/schedr-users anna ben       # asks for each password
    pass show schedr/users | scripts/users-file.py --out ~/schedr-users --stdin
    scripts/users-file.py --out ~/schedr-users --secret schedr-users --namespace etl anna

Writes <out>/users and <out>/users-secret.yaml (0600), then:
    kubectl apply -f <out>/users-secret.yaml

Keep them out of version control: a Secret is only base64, and anyone holding the hashes can
guess passwords offline. `--out` has no default, and a directory inside a git checkout is
warned about. To keep the Secret in git (GitOps), encrypt it first (Sealed Secrets, SOPS).
"""

from __future__ import annotations

import argparse
import base64
import getpass
import os
import sys
from pathlib import Path

import bcrypt


def in_git_checkout(directory: Path) -> Path | None:
    """The git checkout `directory` is in, if any."""
    for parent in [directory.resolve(), *directory.resolve().parents]:
        if (parent / ".git").exists():
            return parent
    return None


# As schedr reads them (crates/schedr-api/src/auth.rs): a longer name is refused.
MAX_NAME = 256
# bcrypt reads at most 72 bytes of a password.
MAX_PASSWORD = 72
# The costs bcrypt takes.
COSTS = range(4, 32)


def usable_name(name: str) -> str | None:
    """Why `name` can't be a user's, or None."""
    if not name or name != name.strip():
        return "empty, or spaces around it"
    if ":" in name:
        return "contains `:`"
    if len(name.encode()) > MAX_NAME or any(not c.isprintable() for c in name):
        return f"longer than {MAX_NAME} bytes, or a control character"
    return None


def usable_password(password: str) -> str | None:
    """Why `password` can't be used, or None."""
    if not password:
        return "empty"
    if len(password.encode()) > MAX_PASSWORD:
        return f"longer than {MAX_PASSWORD} bytes (bcrypt would ignore the rest)"
    return None


def asked(names: list[str]) -> list[tuple[str, str]]:
    """Each name with its password, asked for twice."""
    users = []
    for name in names:
        while True:
            password = getpass.getpass(f"{name}'s password: ")
            if why := usable_password(password):
                print(f"  {why}; again", file=sys.stderr)
                continue
            if getpass.getpass(f"{name}'s password again: ") != password:
                print("  they differ; again", file=sys.stderr)
                continue
            break
        users.append((name, password))
    return users


def read(lines: list[str]) -> list[tuple[str, str]]:
    """`name:password` lines (blank lines and `#` comments skipped)."""
    users = []
    for number, raw in enumerate(lines, 1):
        line = raw.rstrip("\r\n")
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        name, colon, password = line.partition(":")
        if not colon:
            sys.exit(f"line {number} isn't `name:password`")
        if why := usable_password(password):
            sys.exit(f"line {number}: {name}'s password is {why}")
        users.append((name, password))
    return users


def secret(name: str, namespace: str | None, key: str, text: str) -> str:
    """A Secret of `text` under `key`."""
    encoded = base64.b64encode(text.encode()).decode()
    metadata = f"  name: {name}\n" + (f"  namespace: {namespace}\n" if namespace else "")
    return (
        "# The users of schedr's `basic` sign-in (scripts/users-file.py).\n"
        "apiVersion: v1\n"
        "kind: Secret\n"
        "type: Opaque\n"
        f"metadata:\n{metadata}"
        f"data:\n  {key}: {encoded}\n"
    )


def write(path: Path, text: str) -> None:
    """`text` into `path`, readable by its owner only."""
    path.unlink(missing_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as file:
        file.write(text)


def main(argv: list[str] | None = None) -> None:
    """Ask for or read the users, hash their passwords, write both files."""
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n", 1)[0],
        epilog=__doc__.split("\n\n", 1)[1],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("names", nargs="*", help="the users; each password is asked for")
    parser.add_argument(
        "--stdin", action="store_true", help="read `name:password` lines from standard input"
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="where to write (a directory outside version control)",
    )
    parser.add_argument(
        "--secret", default="schedr-users", help="the Secret's name (default: schedr-users)"
    )
    parser.add_argument("--namespace", help="the Secret's namespace (default: kubectl's)")
    parser.add_argument(
        "--key", default="users", help="the Secret's key (default: users, the chart's)"
    )
    parser.add_argument(
        "--cost", type=int, default=12, help="bcrypt's cost, 4-31 (default 12: about 0.25 s)"
    )
    args = parser.parse_args(argv)
    if args.stdin == bool(args.names):
        parser.error("give the users' names, or --stdin (not both)")
    if args.cost not in COSTS:
        parser.error("--cost is 4 to 31")
    users = read(sys.stdin.readlines()) if args.stdin else None
    names = [name for name, _ in users] if users is not None else args.names
    for name in names:
        if why := usable_name(name):
            sys.exit(f"{name!r}: the name is {why}")
    if len(set(names)) != len(names):
        sys.exit("a name is there twice")
    if not names:
        sys.exit("no users")
    if users is None:
        users = asked(names)
    lines = [
        f"{name}:{bcrypt.hashpw(password.encode(), bcrypt.gensalt(args.cost)).decode()}"
        for name, password in users
    ]
    text = "# Who may sign in (scripts/users-file.py): name:bcrypt-hash\n" + "\n".join(lines) + "\n"
    args.out.mkdir(parents=True, exist_ok=True)
    if checkout := in_git_checkout(args.out):
        print(
            f"warning: {args.out} is inside the git checkout {checkout}: don't commit these "
            "files (a Secret is only base64; encrypt it for GitOps)",
            file=sys.stderr,
        )
    write(args.out / "users", text)
    write(args.out / "users-secret.yaml", secret(args.secret, args.namespace, args.key, text))
    print(
        f"{len(lines)} user(s): {args.out / 'users'} and {args.out / 'users-secret.yaml'}\n"
        f"  kubectl apply -f {args.out / 'users-secret.yaml'}\n"
        f"  chart values: webserver.auth.usersSecret.name: {args.secret} (key {args.key})",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
