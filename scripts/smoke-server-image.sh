#!/usr/bin/env bash
# Smoke-tests the schedr server image as a pod runs it: no Python in it; under an arbitrary UID
# in group 0 (as OpenShift does), `schedr master --migrate` migrates and leads, `schedr
# webserver` beside it answers, and SIGTERM stops the master cleanly.
# Usage: scripts/smoke-server-image.sh <container tool> <image>
set -euo pipefail
tool=$1
image=$2
name=schedr-server-smoke-$$
cleanup() { "$tool" rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

if "$tool" run --rm --entrypoint sh "$image" -c 'command -v python3 || command -v python'; then
    echo "the server image has Python"; exit 1
fi
# The roles on one SQLite file in one container (one host, as SQLite needs), in mesh mode
# (no certificates).
"$tool" run -d --name "$name" --user 54321:0 -p 127.0.0.1::8090 \
    -e SCHEDR_CONFIG__DATABASE__URL=sqlite:///var/lib/schedr/schedr.db \
    -e SCHEDR_CONFIG__TLS__MODE=mesh "$image" master --migrate >/dev/null
port=$("$tool" port "$name" 8090/tcp | head -1 | sed 's/.*://')
for _ in $(seq 1 120); do
    curl -sf "http://127.0.0.1:$port/metrics" 2>/dev/null | grep -q '^schedr_master_leading 1' && break
    sleep 0.5
done
curl -sf "http://127.0.0.1:$port/metrics" | grep -q '^schedr_master_leading 1' || {
    echo "the master doesn't lead:"; "$tool" logs "$name"; exit 1
}
"$tool" exec -d -e SCHEDR_CONFIG__OPS__PORT=8091 -e SCHEDR_CONFIG__WEBSERVER__PORT=8080 \
    "$name" schedr webserver
for _ in $(seq 1 60); do
    "$tool" exec "$name" wget -qO- http://127.0.0.1:8091/readyz >/dev/null 2>&1 && break
    sleep 0.5
done
"$tool" exec "$name" wget -qO- http://127.0.0.1:8080/api/v1/dags | grep -q '"dags"' || {
    echo "the webserver doesn't answer:"; "$tool" logs "$name"; exit 1
}
"$tool" stop --time 30 "$name" >/dev/null
code=$("$tool" inspect --format '{{.State.ExitCode}}' "$name")
[ "$code" = 0 ] || { echo "exit code $code after SIGTERM:"; "$tool" logs "$name"; exit 1; }
echo "server image ok: no Python, the master migrated and leading, the webserver answering, stopped cleanly"
