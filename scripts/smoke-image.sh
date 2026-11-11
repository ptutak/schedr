#!/usr/bin/env bash
# Smoke-tests the schedr base image as a pod runs it: `schedr standalone` under an arbitrary
# UID in group 0 (as OpenShift does), ready on the ops port, its CLI answering, and SIGTERM
# stopping it cleanly. Usage: scripts/smoke-image.sh <container tool> <image> [slim]
# `slim` (images/schedr-slim, decision 383): no task libraries and no connection clients, and
# a DAG without connections runs.
set -euo pipefail
tool=$1
image=$2
kind=${3:-full}
name=schedr-smoke-$$
cleanup() { "$tool" rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

"$tool" run --rm --entrypoint test "$image" -f /usr/share/schedr/docs/index.html || {
    echo "the image has no documentation for the UI's /docs"; exit 1
}
# schedr's licence and its crates', installed from the wheel with the package.
"$tool" run --rm --entrypoint python "$image" -c 'from importlib.metadata import distribution
files = [str(f) for f in distribution("schedr").files or []]
assert any(f.endswith("licenses/LICENSE-THIRD-PARTY") for f in files), "no LICENSE-THIRD-PARTY"' || {
    echo "the base image has no LICENSE-THIRD-PARTY"; exit 1
}
# Without --migrate (as the chart runs it), a fresh database is refused...
if "$tool" run --rm --user 54321:0 "$image" >/dev/null 2>&1; then
    echo "a database not migrated wasn't refused"; exit 1
fi
# ...and with it, migrated at start.
"$tool" run -d --name "$name" --user 54321:0 -p 127.0.0.1::8090 "$image" \
    standalone --listen 0.0.0.0 --migrate >/dev/null
port=$("$tool" port "$name" 8090/tcp | head -1 | sed 's/.*://')
for _ in $(seq 1 120); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/readyz")" = 200 ]; then
        break
    fi
    sleep 0.5
done
curl -sf "http://127.0.0.1:$port/readyz" >/dev/null || {
    echo "not ready:"; "$tool" logs "$name"; exit 1
}
curl -sf "http://127.0.0.1:$port/metrics" | grep -q '^schedr_master_leading 1' || {
    echo "the master doesn't lead:"; "$tool" logs "$name"; exit 1
}
"$tool" exec "$name" schedr workers list --server http://127.0.0.1:8080 >/dev/null
"$tool" exec "$name" python3 -c 'import schedr'
if [ "$kind" = slim ]; then
    for client in psql mariadb mongoexport ssh curl; do
        if "$tool" exec "$name" sh -c "command -v $client" >/dev/null 2>&1; then
            echo "the slim image has $client"; exit 1
        fi
    done
    # A DAG without connections runs to its end.
    "$tool" exec "$name" sh -c 'mkdir -p /tmp/dags && cat > /tmp/dags/hello.py <<PY
from schedr import CommandOperator, Dag, task


@task
def greet() -> str:
    return "hello"


with Dag("hello"):
    CommandOperator("echo", ["echo", "slim"]) >> greet()
PY
cd /tmp/dags && schedr dags test hello' | grep -q "run .*: success" || {
        echo "the slim image didn't run a DAG"; exit 1
    }
fi
# The full image has every connection's command-line client (on glibc and on Alpine alike).
if [ "$kind" != slim ]; then
    for client in psql mysql mariadb mysqldump mongoexport mongodump ssh scp curl; do
        "$tool" exec "$name" sh -c "command -v $client" >/dev/null 2>&1 || {
            echo "the image has no $client"; exit 1
        }
    done
    # pg_dump refuses a server of a newer major version: 18, as the newest Postgres.
    "$tool" exec "$name" pg_dump --version | grep -q ' 18\.' || {
        echo "the image's pg_dump isn't PostgreSQL 18's: $("$tool" exec "$name" pg_dump --version)"
        exit 1
    }
fi
# Every library of the image imports (a native library missing on Alpine shows here), and
# pymongo has its C extensions.
[ "$kind" = slim ] || "$tool" exec "$name" python3 -c '
import aiohttp, dateutil, dns.resolver, httpx, ncclient, netaddr, netmiko, orjson, paramiko
import polars, pymongo, requests, tenacity, yaml, bson
assert pymongo.has_c() and bson.has_c(), "pymongo without its C extensions"
polars.DataFrame({"a": [1, 2]}).select(polars.col("a").sum())
'
"$tool" stop --time 30 "$name" >/dev/null
code=$("$tool" inspect --format '{{.State.ExitCode}}' "$name")
[ "$code" = 0 ] || { echo "exit code $code after SIGTERM:"; "$tool" logs "$name"; exit 1; }
if [ "$kind" = slim ]; then
    echo "slim image ok: ready, leading, CLI answering, no clients, a DAG run, stopped cleanly"
else
    echo "image ok: ready, leading, CLI answering, the libraries importing, stopped cleanly"
fi
