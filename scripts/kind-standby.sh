#!/usr/bin/env bash
# Hot standby masters (decision 523) on a LOCAL kind cluster of its own, schedr-standby: the chart
# with master.replicas 2 on Postgres. The second master waits, running and not ready (not in an
# init container); the leading master's pod is killed during a task with retries=0, the standby
# that was waiting leads, that same attempt finishes, no worker is lost, and a new standby waits;
# an upgrade replaces them all and one leads again.
#
# The context is always kind-schedr-standby; any other CLUSTER is refused. The cluster is created
# when missing and deleted at the end by the run that created it (KEEP=1 keeps it). Not part of
# `make check`; on CI by hand, the Kind workflow, on a release's images. Shared pieces live in
# scripts/kind-lib.sh.
#
# Usage: scripts/kind-standby.sh     (the images `make image` and `make server-image` built)
#   IMAGE, SERVER_IMAGE  as the container tool names them, e.g. a release's:
#                        IMAGE=ghcr.io/ptutak/schedr:1.2.0 SERVER_IMAGE=ghcr.io/ptutak/schedr-server:1.2.0
#   CONTAINER=podman
set -euo pipefail

if [ "${CLUSTER:-schedr-standby}" != schedr-standby ]; then
    echo "FAILED: scripts/kind-standby.sh only runs on kind cluster schedr-standby (CLUSTER=$CLUSTER)"
    exit 1
fi
CLUSTER=schedr-standby
CONTEXT=kind-$CLUSTER
NAMESPACE=schedr
WORKER_IMAGE=localhost/schedr-kind-standby:1
# The lease period (30 s), the readiness probe's period (10 s) and the Service's update.
TAKEOVER_S=60
ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=kind-lib.sh
source "$ROOT/scripts/kind-lib.sh"
WORK=$(mktemp -d)
created=
cleanup() {
    rm -rf "$WORK"
    if [ -n "$created" ] && [ -z "${KEEP:-}" ]; then
        kind delete cluster --name "$CLUSTER" || true
    fi
}
trap cleanup EXIT

say "kind cluster $CLUSTER (context $CONTEXT)"
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    created=1 # first: one that failed halfway is removed too
    kind create cluster --name "$CLUSTER"
fi
require_local_context
start_kind_node
# As a run stopped between the two may have left it.
kubectl --context "$CONTEXT" uncordon "$CLUSTER-control-plane" >/dev/null
kubectl --context "$CONTEXT" get namespace "$NAMESPACE" >/dev/null 2>&1 \
    || kubectl --context "$CONTEXT" create namespace "$NAMESPACE"

say "images: the workers' ($IMAGE with a slow DAG), the server's"
cat > "$WORK/slow.py" <<'PYTHON'
import time

from schedr import Dag, task

with Dag("slow"):

    @task(retries=0)
    def nap() -> str:
        time.sleep(100)
        return "rested"

    nap()
PYTHON
cat > "$WORK/Dockerfile" <<DOCKERFILE
FROM $IMAGE
COPY slow.py /dags/current/
DOCKERFILE
"$TOOL" build -q -t "$WORKER_IMAGE" "$WORK" >/dev/null
load_images "$WORKER_IMAGE" "$SERVER_IMAGE"

say "Postgres, then the chart with two masters"
# From a clean release and database: a kept cluster's old masters would pass for the new ones.
h uninstall schedr --wait >/dev/null 2>&1 || true
k delete deployment,statefulset,service,secret,serviceaccount,poddisruptionbudget,configmap \
    -l app.kubernetes.io/instance=schedr --wait --ignore-not-found >/dev/null
k delete pvc data-schedr-worker-0 data-schedr-worker-1 --ignore-not-found --wait >/dev/null
k delete statefulset postgres --ignore-not-found --wait >/dev/null
k wait --for=delete pod -l "app.kubernetes.io/instance=schedr" --timeout=180s >/dev/null 2>&1 || true
k wait --for=delete pod postgres-0 --timeout=120s >/dev/null 2>&1 || true
apply_postgres
values=(
    --set mode=distributed --set tls.mode=app --set master.replicas=2
    --set image.repository="${WORKER_IMAGE%:*}" --set image.tag="${WORKER_IMAGE##*:}"
    --set image.pullPolicy=Never
    --set master.image.repository="${SERVER_IMAGE%:*}" --set master.image.tag="${SERVER_IMAGE##*:}"
    --set master.image.pullPolicy=Never
    --set webserver.image.repository="${SERVER_IMAGE%:*}" --set webserver.image.tag="${SERVER_IMAGE##*:}"
    --set webserver.image.pullPolicy=Never
    --set ui.enabled=false
    --set database.postgres.host=postgres --set database.postgres.name=postgres
    --set database.postgres.existingSecret=schedr-pg
    --set master.resources.requests.cpu=100m
    "${fit[@]}"
)
# No --wait: a standby is never ready.
h upgrade --install schedr "$ROOT/charts/schedr" "${values[@]}" >/dev/null

# The masters, one a line: name, ready (True/False), whether its container runs. One read of the
# API, whose failure is the caller's to see.
masters() {
    k get pods -l app.kubernetes.io/instance=schedr,app.kubernetes.io/component=master -o json \
        | python3 -c '
import json, sys
for pod in json.load(sys.stdin)["items"]:
    if pod["metadata"].get("deletionTimestamp"):
        continue
    status = pod["status"]
    ready = next((c["status"] for c in status.get("conditions", []) if c["type"] == "Ready"), "False")
    running = any("running" in c["state"] for c in status.get("containerStatuses", []))
    print(pod["metadata"]["name"], ready, running)'
}
# Sets LEADER and STANDBY from one read: two masters, one ready, the other running and not ready.
one_leads_one_waits() {
    local state
    state=$(masters) || return 1
    [ "$(wc -l <<<"$state")" = 2 ] || return 1
    LEADER=$(awk '$2 == "True" {print $1}' <<<"$state")
    STANDBY=$(awk '$2 == "False" && $3 == "True" {print $1}' <<<"$state")
    [ "$(wc -w <<<"$LEADER")" = 1 ] && [ "$(wc -w <<<"$STANDBY")" = 1 ]
}
settled() {
    for _ in $(seq 1 120); do
        one_leads_one_waits && return 0
        sleep 2
    done
    masters || true
    fail "$1: not one master leading and one waiting"
}
cli() { k exec deploy/schedr-webserver -c schedr -- schedr "$@" --server http://127.0.0.1:8080; }
# Both workers active, waited for (a worker's wait before it tries again is up to half its lease).
workers_active() {
    local listed
    for _ in $(seq 1 60); do
        if listed=$(cli workers list --json 2>/dev/null) && python3 -c '
import json, sys
states = [w["state"] for w in json.load(sys.stdin)["workers"]]
sys.exit(states != ["active", "active"])' <<<"$listed"; then
            return 0
        fi
        sleep 2
    done
    cli workers list || true
    fail "$1: not two active workers"
}
# Sets RUN: a run of `slow`, its ID read, never empty.
trigger() {
    local answer
    answer=$(cli runs trigger slow --json) || fail "runs trigger slow failed"
    RUN=$(python3 -c 'import json, sys; print(json.load(sys.stdin)["result"]["run_id"])' <<<"$answer")
    [ -n "$RUN" ] || fail "runs trigger slow gave no run ($answer)"
}
# The run's task's attempts, one a line: worker, status, start.
attempts() {
    local answer
    answer=$(cli tasks attempts "$RUN" nap --json) || return 1
    python3 -c '
import json, sys
for a in json.load(sys.stdin)["attempts"]:
    print(a["worker_id"], a["status"], a["started_at"])' <<<"$answer"
}
run_succeeds() {
    local answer
    for _ in $(seq 1 120); do
        if answer=$(cli runs show "$RUN" --json 2>/dev/null); then
            case $(python3 -c 'import json, sys; print(json.load(sys.stdin)["run"]["state"])' <<<"$answer") in
                success) return 0 ;;
                failed | cancelled) fail "$1: the run didn't succeed" ;;
            esac
        fi
        sleep 2
    done
    fail "$1: the run didn't end in time"
}

settled "after the install"
k rollout status deployment/schedr-webserver --timeout=180s
k rollout status statefulset/schedr-worker --timeout=300s
workers_active "after the install"
found=
for _ in $(seq 1 60); do
    if listed=$(cli dags list 2>/dev/null) && grep -q slow <<<"$listed"; then found=1; break; fi
    sleep 2
done
[ -n "$found" ] || fail "the DAG slow wasn't parsed"

say "the leading master crashes while a task runs (retries=0)"
trigger
started=
for _ in $(seq 1 60); do
    if listed=$(attempts) && [ -n "$listed" ] && ! grep -q " None$" <<<"$listed"; then started=1; break; fi
    sleep 1
done
[ -n "$started" ] || fail "the slow task didn't start"
# Read again now: who leads may have changed since the install.
settled "before the crash"
leader=$LEADER
standby=$STANDBY
echo "ok: $leader leads, $standby waits as a hot standby"
# No replacement pod until the standby leads: it would often take the lease first, as a standby
# looks for it every 10 s.
kubectl --context "$CONTEXT" cordon "$CLUSTER-control-plane" >/dev/null
# A crash: the container's first process killed on the node (SIGKILL, so no goodbye and the lease
# kept), then the pod removed before its container restarts. `kubectl delete --force` alone stops
# it cleanly, giving the lease up.
at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
pid=$("$TOOL" exec "$CLUSTER-control-plane" sh -c \
    "crictl inspect -o go-template --template '{{.info.pid}}' \$(crictl ps -q --pod \$(crictl pods -q --name $leader))")
[[ $pid =~ ^[0-9]+$ ]] || fail "no process of $leader (${pid:-none})"
"$TOOL" exec "$CLUSTER-control-plane" kill -KILL "$pid" || fail "the process of $leader wasn't killed"
killed=$(date +%s)
k delete pod "$leader" --force --grace-period=0 >/dev/null 2>&1 || fail "the pod $leader wasn't removed"
until state=$(masters) && grep -qx "$standby True True" <<<"$state"; do
    [ $(($(date +%s) - killed)) -le "$TAKEOVER_S" ] || { masters || true; fail "$standby didn't lead within $TAKEOVER_S s"; }
    sleep 1
done
took=$(($(date +%s) - killed))
# The lease (30 s, renewed every few) had to run out: a takeover at once wasn't after a crash.
[ "$took" -ge 5 ] || fail "$standby led after $took s: the leader gave up its lease, no crash"
for worker in schedr-worker-0 schedr-worker-1; do
    logged=$(k logs "$worker" -c schedr --since-time="$at")
    if grep -q "this master is stopping" <<<"$logged"; then fail "the leader stopped cleanly, said $worker"; fi
done
echo "ok: $standby leads $took s after the crash"
workers_active "after the crash"
echo "ok: both workers are back $(($(date +%s) - killed)) s after the crash"
kubectl --context "$CONTEXT" uncordon "$CLUSTER-control-plane" >/dev/null

run_succeeds "after the crash"
listed=$(attempts)
[ "$(wc -l <<<"$listed")" = 1 ] || fail "the task's attempt wasn't kept: $listed"
workers_active "after the run"
settled "after the crash"
echo "ok: the same attempt finished, both workers stayed, a new standby waits"

say "an upgrade replaces both masters"
before=$(masters | awk '{print $1}' | sort)
[ "$(wc -l <<<"$before")" = 2 ] || fail "not two masters before the upgrade: $before"
h upgrade schedr "$ROOT/charts/schedr" "${values[@]}" --set master.podAnnotations.upgraded=yes >/dev/null
replaced=
for _ in $(seq 1 60); do
    if after=$(masters | awk '{print $1}' | sort) && [ "$(wc -l <<<"$after")" = 2 ] \
        && [ -z "$(comm -12 <(printf '%s\n' "$before") <(printf '%s\n' "$after"))" ]; then
        replaced=1
        break
    fi
    sleep 2
done
[ -n "$replaced" ] || fail "an old master is still there"
settled "after the upgrade"
workers_active "after the upgrade"
trigger
run_succeeds "after the upgrade"
echo "ok: one leads, one waits, the workers are back and a run succeeds"

say "done: hot standby masters"
