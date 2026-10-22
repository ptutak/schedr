#!/usr/bin/env bash
# Smoke test of a built wheel: what PLAN.md asks of M1 ("M1 is done"), as a new user does it.
#
#   scripts/smoke-wheel.sh dist/schedr-*.whl [python]
#
# Installs the wheel into a fresh venv (made with `python`, default python3; it must be 3.14+)
# and, without activating the venv and with no other Python that has schedr on PATH:
#   1. `schedr dags check` and `schedr dags test` on the example DAG (examples/sync.py);
#   2. `schedr standalone` on ports the system picks, then `dags list`, `dags unpause`,
#      `runs trigger --for ...` and `runs list` until that run succeeds.
# Exits non-zero, with the standalone's log, if any step fails.

set -euo pipefail

wheel=${1:?usage: scripts/smoke-wheel.sh <wheel> [python]}
python=${2:-python3}
repo=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
standalone_pid=

cleanup() {
    if [[ -n $standalone_pid ]]; then
        kill "$standalone_pid" 2>/dev/null || true
        wait "$standalone_pid" 2>/dev/null || true
    fi
    rm -rf "$work"
}
trap cleanup EXIT

fail() {
    echo "smoke test failed: $*" >&2
    if [[ -f $work/standalone.log ]]; then
        echo "--- standalone log ---" >&2
        tail -n 50 "$work/standalone.log" >&2
    fi
    exit 1
}

step() { echo "== $*"; }

step "installing $(basename "$wheel") into a fresh venv"
"$python" -m venv "$work/venv"
"$work/venv/bin/pip" install --quiet --disable-pip-version-check "$wheel"
schedr=$work/venv/bin/schedr
"$schedr" --version

# A clean environment, as for a new user: the venv isn't activated, and only the system's
# directories are on PATH, so the venv's Python is found next to the binary.
mkdir -p "$work/dags" "$work/home"
cp "$repo/examples/sync.py" "$work/dags/sync.py"
run() {
    env -i PATH=/usr/bin:/bin HOME="$work/home" TZ=UTC "$@"
}

step "dags check"
run "$schedr" dags check "$work/dags/sync.py" || fail "dags check"

step "dags test"
run "$schedr" dags test sync_customers --path "$work/dags/sync.py" \
    --for 2026-10-15T01:00:00Z || fail "dags test"

step "standalone"
# Port 0: standalone binds free ports itself and logs them in its banner (picking free ports
# here and passing them on would race with anything else binding meanwhile).
# Started directly (not through `run`, a function, which `&` would put in a subshell), so
# $! is the standalone's own PID: `env` execs it.
env -i PATH=/usr/bin:/bin HOME="$work/home" TZ=UTC \
    SCHEDR_DAG_PATH="$work/dags" \
    SCHEDR_CONFIG__WEBSERVER__PORT=0 \
    SCHEDR_CONFIG__MASTER__GRPC_PORT=0 \
    SCHEDR_CONFIG__WORKER__DATA_DIR="$work/data" \
    "$schedr" standalone >"$work/standalone.log" 2>&1 &
standalone_pid=$!

server=
for _ in $(seq 120); do
    # No match yet is expected while standalone starts (and fatal under pipefail without `||`).
    server=$(grep -o 'API on http://[0-9.:]*' "$work/standalone.log" | head -n 1 |
        sed 's/^API on //' || true)
    [[ -n $server ]] && break
    kill -0 "$standalone_pid" 2>/dev/null || fail "standalone exited"
    sleep 0.5
done
[[ -n $server ]] || fail "standalone logged no banner"

cli() {
    run "$schedr" "$1" --server "$server" "${@:2}"
}

for _ in $(seq 120); do
    if cli dags list 2>/dev/null | grep -q '^sync_customers '; then
        break
    fi
    kill -0 "$standalone_pid" 2>/dev/null || fail "standalone exited"
    sleep 0.5
done
cli dags list || fail "dags list"
cli dags list | grep -q '^sync_customers ' || fail "sync_customers wasn't found"

cli dags unpause sync_customers || fail "dags unpause"
cli runs trigger sync_customers --for 2026-10-15T01:00:00Z || fail "runs trigger"

state=
for _ in $(seq 240); do
    state=$(cli runs list sync_customers --for 2026-10-15T01:00:00Z --json |
        "$python" -c 'import json, sys; runs = json.load(sys.stdin)["runs"]; print(runs[0]["state"] if runs else "")') ||
        fail "runs list"
    case $state in
        success) break ;;
        failed | cancelled) fail "the run ended $state" ;;
    esac
    sleep 0.5
done
cli runs list sync_customers || fail "runs list"
[[ $state == success ]] || fail "the run didn't succeed in time (state: ${state:-none})"

step "ok: the wheel runs the example DAG"
