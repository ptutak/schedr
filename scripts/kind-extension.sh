#!/usr/bin/env bash
# Runs the Kubernetes extension's DAGs (examples/extensions/kubernetes_extension.py, M5.2i2) on a
# LOCAL kind cluster: a standalone here (`make standalone`) whose worker has the plugin
# (examples/extensions/kubernetes/schedr-ext-kubernetes), each attempt a Job in the cluster.
# Checks: the pod's log in the attempt's log while it runs, its output, a failure's reason, a
# timeout deleting the Job, each Job deleted by its cleanup, and a worker lost with its plugin
# (both killed) leaving a Job its cleanup finds by the handle and deletes. Every kubectl call
# names the kind cluster's context. Port 8080 must be free.
#
# Usage: scripts/kind-extension.sh
#   CLUSTER=schedr-test  the kind cluster (created if missing, kept afterwards)
#   CONTAINER=podman     the container tool
set -euo pipefail
CLUSTER=${CLUSTER:-schedr-test}
CONTEXT=kind-$CLUSTER
NAMESPACE=schedr-ext
ROOT=$(cd "$(dirname "$0")/.." && pwd)
# The standalone it starts, whatever SCHEDR_SERVER the shell has (agy's review of M5.2i7).
export SCHEDR_SERVER=http://127.0.0.1:8080
# shellcheck source-path=SCRIPTDIR
# shellcheck source=kind-lib.sh
source "$ROOT/scripts/kind-lib.sh"
WORK=$(mktemp -d)
SCHEDR=$ROOT/target/debug/schedr
PLUGIN=$ROOT/examples/extensions/kubernetes/schedr-ext-kubernetes
STANDALONE_PGID=

stop_standalone() {
    if [ -n "$STANDALONE_PGID" ]; then
        kill -s TERM -- "-$STANDALONE_PGID" 2>/dev/null || true
        for _ in $(seq 1 50); do
            kill -0 -- "-$STANDALONE_PGID" 2>/dev/null || break
            sleep 0.2
        done
        kill -s KILL -- "-$STANDALONE_PGID" 2>/dev/null || true
        STANDALONE_PGID=
    fi
}
finish() {
    local code=$?
    stop_standalone
    if [ "$code" != 0 ]; then
        echo "---- the standalone's log (tail) ----"
        tail -40 "$WORK/standalone.log" 2>/dev/null || true
        echo "---- the attempts' logs ----"
        tail -n 20 "$WORK"/data/logs/*/*.log 2>/dev/null || true
    fi
    # The parsed DAG code is kept read-only.
    chmod -R u+w "$WORK" 2>/dev/null || true
    rm -rf "$WORK"
}
trap finish EXIT

if curl -s -o /dev/null --max-time 2 http://127.0.0.1:8080/; then
    echo "FAILED: something already listens on 8080 (a standalone?): stop it first"
    exit 1
fi

say "kind cluster $CLUSTER (context $CONTEXT), namespace $NAMESPACE"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
require_local_context
start_kind_node
k get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl --context "$CONTEXT" create namespace "$NAMESPACE"
k delete jobs -l app.kubernetes.io/managed-by=schedr --ignore-not-found >/dev/null

mkdir -p "$WORK/dags"
cp "$ROOT/examples/extensions/kubernetes_extension.py" "$WORK/dags/"
# The Dashboard's address only names the links (decision 513): none needs to answer here.
export SCHEDR_CONFIG__EXTENSIONS__KUBERNETES__COMMAND="[\"$PLUGIN\", \"--namespace\", \"$NAMESPACE\", \"--context\", \"$CONTEXT\", \"--dashboard\", \"https://dashboard.example\"]"
# Its metrics' figures sent at once, not once a minute.
export SCHEDR_HIDDEN__WORKER__EXTENSION_FIGURES_INTERVAL=2s
export SCHEDR_CONFIG__EXTENSIONS__KUBERNETES__VERSIONS='["1.0"]'

start_standalone() {
    say "a standalone with the plugin (log: $WORK/standalone.log)"
    for _ in $(seq 1 600); do
        # Started again while it isn't running: after a kill, it refuses until the killed
        # master's lease runs out.
        if [ -z "$STANDALONE_PGID" ] || ! kill -0 "$STANDALONE_PGID" 2>/dev/null; then
            setsid make -C "$ROOT" --no-print-directory standalone DAGS="$WORK/dags" \
                STANDALONE_DIR="$WORK/data" LISTEN=127.0.0.1 >>"$WORK/standalone.log" 2>&1 </dev/null &
            STANDALONE_PGID=$!
        fi
        if "$SCHEDR" dags list 2>/dev/null | grep -q pod_long; then
            return
        fi
        sleep 1
    done
    tail -30 "$WORK/standalone.log"
    fail "the standalone didn't find the DAGs"
}

# The state of DAG $1's newest run.
run_state() {
    "$SCHEDR" runs list "$1" --json 2>/dev/null \
        | python3 -c 'import json,sys; r=json.load(sys.stdin)["runs"]; print(r[0]["state"] if r else "none")'
}

# Waits until DAG $1's newest run is $2 (at most $3 seconds).
wait_run() {
    local state
    for _ in $(seq 1 "$3"); do
        state=$(run_state "$1")
        [ "$state" = "$2" ] && return
        case "$state" in success | failed) fail "$1 ended $state, not $2" ;; esac
        sleep 1
    done
    fail "$1 isn't $2 after $3 s (it is $(run_state "$1"))"
}

# The schedr Jobs left in the namespace; fails when kubectl does (never an empty answer).
jobs_left() {
    k get jobs -l app.kubernetes.io/managed-by=schedr -o name
}

wait_no_jobs() {
    local left
    for _ in $(seq 1 "$1"); do
        if left=$(jobs_left) && [ -z "$left" ]; then
            return
        fi
        sleep 1
    done
    fail "Jobs left after $1 s: $(jobs_left)"
}

logs() {
    cat "$WORK"/data/logs/*/*.log 2>/dev/null || true
}

start_standalone

say "pod_runs: the log while it runs, the output, the Job deleted"
"$SCHEDR" runs trigger pod_runs >/dev/null
seen_running=0
for _ in $(seq 1 180); do
    if logs | grep -q "row 1"; then
        [ "$(run_state pod_runs)" = running ] && seen_running=1
        break
    fi
    sleep 1
done
[ "$seen_running" = 1 ] || fail "the pod's log wasn't in the attempt's while it ran"
wait_run pod_runs success 180
logs | grep -q "token is " || fail "the secret didn't reach the pod"
wait_no_jobs 60
echo "ok: streamed while running, succeeded, its Job deleted"

say "pod_runs: its phases and links on the attempt, its metrics and page (decision 513)"
run=$("$SCHEDR" runs list pod_runs --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["runs"][0]["run_id"])')
reported=$(curl -s "http://127.0.0.1:8080/api/v1/runs/$run/tasks/count/attempts" | python3 -c '
import json, sys
a = json.load(sys.stdin)["attempts"][0]
print(" ".join(s["phase"] for s in a["statuses"]), "|", " ".join(l["label"] for l in a["links"]))')
[ "$reported" = "creating pending running succeeded | Job Pod" ] || fail "pod_runs's reports: $reported"
for _ in $(seq 1 30); do
    panels=$(curl -s http://127.0.0.1:8080/api/v1/extensions/kubernetes \
        | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("panels", [])))' 2>/dev/null || true)
    [ "$panels" = 6 ] && break
    sleep 1
done
[ "$panels" = 6 ] || fail "the extension's page has $panels panels, not 6"
ops=$(grep -o 'ops on http://[0-9.:]*' "$WORK/standalone.log" | tail -1 | sed 's|ops on ||; s|0.0.0.0|127.0.0.1|')
curl -s "$ops/metrics" | grep -q '^schedr_extension_kubernetes_jobs_started_total{namespace="'"$NAMESPACE"'"' \
    || fail "the worker's /metrics has no jobs_started"
echo "ok: phases and links on the attempt, six panels, jobs_started on /metrics"

say "pod_fails: the pod's reason"
"$SCHEDR" runs trigger pod_fails >/dev/null
wait_run pod_fails failed 180
run=$("$SCHEDR" runs list pod_fails --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["runs"][0]["run_id"])')
error=$(curl -s "http://127.0.0.1:8080/api/v1/runs/$run/tasks/fail/attempts" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["attempts"][0]["error_summary"]["message"])')
[ "$error" = "the pod ended Error with exit code 3" ] || fail "pod_fails's error: $error"
wait_no_jobs 60
echo "ok: failed with exit code 3, its Job deleted"

say "pod_timeout: the Job deleted when the task times out"
"$SCHEDR" runs trigger pod_timeout >/dev/null
wait_run pod_timeout failed 120
logs | grep -q '"message":"stopped: timeout"' || fail "the plugin wasn't told it timed out"
logs | grep -q '"message":"deleted job ' || fail "the plugin didn't delete its Job on the timeout"
wait_no_jobs 60
echo "ok: timed out, its Job deleted"

say "pod_long: a worker lost with its plugin; the cleanup finds the Job by its handle"
"$SCHEDR" runs trigger pod_long >/dev/null
for _ in $(seq 1 120); do
    [ -n "$(jobs_left)" ] && break
    sleep 1
done
[ -n "$(jobs_left)" ] || fail "pod_long's Job wasn't created"
job=$(jobs_left)
# Only this script's plugin: a child of the schedr process in this script's standalone group
# (the plugin runs in a group of its own, its parent the worker).
schedr_pids=$(pgrep -g "$STANDALONE_PGID" -x schedr | paste -sd, -)
[ -n "$schedr_pids" ] || fail "the standalone's schedr process wasn't found"
plugin_pids=$(pgrep -P "$schedr_pids" -f schedr-ext-kubernetes || true)
[ -n "$plugin_pids" ] || fail "the plugin's process wasn't found"
# A host lost at once: the worker and its plugin killed, nothing deletes the Job.
kill -s KILL -- "-$STANDALONE_PGID" 2>/dev/null || true
for pid in $plugin_pids; do kill -s KILL "$pid" 2>/dev/null || true; done
STANDALONE_PGID=
sleep 2
[ "$(jobs_left)" = "$job" ] || fail "the Job should outlive its worker ($(jobs_left))"
start_standalone
wait_no_jobs 300
logs | grep -q "deleted job ${job#job.batch/}" || fail "the Job wasn't deleted by the cleanup's handle"
echo "ok: $job deleted by its cleanup after the worker was lost"

say "all passed"
