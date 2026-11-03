#!/usr/bin/env bash
# `make demo-check`: the connections demo (examples/demo) started in the background, its DAG
# run in both deployments, every task required to succeed, `schedr connections check` and a
# `test` of every connection from both, then the demo stopped (DEMO_KEEP=1 leaves it up).
# COMPOSE is the compose command with its file (the Makefile's DEMO_COMPOSE).
#
# What M4.7 promised: the same DAG reads each connection's values from two environments'
# storages, opened from Python and from a command.

set -euo pipefail

read -r -a compose <<< "${COMPOSE:?the compose command, e.g. podman-compose -f examples/demo/compose.yaml}"
deployments=(dev prod)
connections=(warehouse orders_db ledger events api box deployment)
# Each task and what its result says (`{env}`: the deployment's name).
python_tasks=(
    'which_deployment={env}'
    "postgres_from_python=hello from {env}'s Postgres"
    "mysql_from_python=hello from {env}'s mysql"
    "mariadb_from_python=hello from {env}'s mariadb"
    "mongo_from_python=hello from {env}'s Mongo"
    'http_from_python=true'
    'ssh_from_python=logged in as demo'
)
command_tasks=(
    "postgres_from_psql=hello from {env}'s Postgres"
    "mysql_from_its_client=hello from {env}'s mysql"
    "mariadb_from_its_client=hello from {env}'s mariadb"
    "mongo_from_mongoexport=hello from {env}'s Mongo"
    'http_from_curl="authenticated": true'
    'ssh_from_ssh=logged in as demo'
)

stop() {
    if [[ ${DEMO_KEEP:-} != 1 ]]; then
        "${compose[@]}" down >/dev/null 2>&1 || true
    fi
}
trap stop EXIT

on() { "${compose[@]}" exec -T "$1" schedr "${@:2}"; }

# Until `what` succeeds, at most `seconds`.
until_ok() {
    local seconds=$1 what=$2
    shift 2
    local deadline=$((SECONDS + seconds))
    until "$@" >/dev/null 2>&1; do
        if ((SECONDS > deadline)); then
            echo "demo-check: $what not after ${seconds} s" >&2
            return 1
        fi
        sleep 2
    done
}

"${compose[@]}" up -d
parsed() {
    on "$1" dags list --json | python3 -c \
        'import json, sys; sys.exit("connections_demo" not in json.dumps(json.load(sys.stdin)))'
}
for env in "${deployments[@]}"; do
    # A deployment that stopped (its start failed) won't parse: its log, at once.
    deadline=$((SECONDS + 300))
    until parsed "$env" >/dev/null 2>&1; do
        if ! "${compose[@]}" exec -T "$env" true >/dev/null 2>&1; then
            echo "demo-check: $env isn't running; its log:" >&2
            "${compose[@]}" logs "$env" 2>&1 | tail -30 >&2
            exit 1
        fi
        if ((SECONDS > deadline)); then
            echo "demo-check: $env's DAG not parsed after 300 s" >&2
            exit 1
        fi
        sleep 2
    done
done

failed=0
declare -A run
for env in "${deployments[@]}"; do
    run[$env]=$(on "$env" runs trigger connections_demo --json | python3 -c \
        'import json, sys; print(json.load(sys.stdin)["result"]["run_id"])')
done
for env in "${deployments[@]}"; do
    finished() {
        on "$1" runs show "$2" --json | python3 -c \
            'import json, sys; sys.exit(json.load(sys.stdin)["run"]["state"] in ("queued", "running"))'
    }
    until_ok 600 "$env's run finished" finished "$env" "${run[$env]}"
    echo "== $env: run ${run[$env]}"
    on "$env" runs show "${run[$env]}"
    state=$(on "$env" runs show "${run[$env]}" --json | python3 -c \
        'import json, sys; print(json.load(sys.stdin)["run"]["state"])')
    if [[ $state != success ]]; then
        echo "demo-check: $env's run ended $state" >&2
        failed=1
    fi
    # Each task's own result, naming its deployment: a Python task's output, a command's log
    # (a run succeeds with tasks skipped, and a client may print nothing).
    for expected in "${python_tasks[@]}" "${command_tasks[@]}"; do
        task=${expected%%=*}
        want=${expected#*=}
        want=${want//\{env\}/$env}
        if [[ " ${python_tasks[*]} " == *" $expected "* ]]; then
            got=$(on "$env" tasks output "${run[$env]}" "$task" 2>&1 || true)
        else
            got=$(on "$env" tasks logs "${run[$env]}" "$task" 2>&1 || true)
        fi
        if [[ $got != *"$want"* ]]; then
            echo "demo-check: $env's $task didn't say \"$want\": $got" >&2
            failed=1
        fi
    done
done

for env in "${deployments[@]}"; do
    echo "== $env: connections check"
    on "$env" connections check || failed=1
    for name in "${connections[@]}"; do
        echo "== $env: connections test $name"
        on "$env" connections test "$name" || failed=1
    done
done

if ((failed)); then
    echo "demo-check: failed (DEMO_KEEP=1 to look at it: make demo-down after)" >&2
    exit 1
fi
echo "demo-check: every connection works from both deployments"
