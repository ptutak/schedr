#!/usr/bin/env bash
# Smoke-tests the schedr base image as a pod runs it: `schedr standalone` under an arbitrary
# UID in group 0 (as OpenShift does), ready on the ops port, its CLI answering, and SIGTERM
# stopping it cleanly. Usage: scripts/smoke-image.sh <container tool> <image>
set -euo pipefail
tool=$1
image=$2
name=schedr-smoke-$$
cleanup() { "$tool" rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

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
"$tool" stop --time 30 "$name" >/dev/null
code=$("$tool" inspect --format '{{.State.ExitCode}}' "$name")
[ "$code" = 0 ] || { echo "exit code $code after SIGTERM:"; "$tool" logs "$name"; exit 1; }
echo "image ok: ready, leading, CLI answering, stopped cleanly"
