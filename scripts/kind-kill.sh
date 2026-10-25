#!/usr/bin/env bash
# Kills schedr during runs on the LOCAL kind cluster schedr-test and checks the failure rules
# (see the guide, "Killing the master or a worker"):
#   - the master pod, during a task with retries=0: that same attempt finishes; no worker is lost
#   - a worker pod, during a task with retries=1: the attempt is lost (WorkerRestarted) and the
#     retry succeeds; the other worker stays active
#   - standalone's pod, on SQLite: an earlier run is still there; the killed task is lost
#     (WorkerRestarted) and its retry succeeds
# A kill is `kubectl delete pod --force --grace-period=0` (no drain). A plain delete is a
# drain, which scripts/kind-distributed.sh and scripts/kind-mesh.sh already check.
#
# The context is always kind-schedr-test. Any other CLUSTER is refused, so this cannot be
# pointed at another cluster. Not part of `make check` or CI.
#
# Usage: scripts/kind-kill.sh     (the images `make image` and `make server-image` built)
#   IMAGE, SERVER_IMAGE  as the container tool names them (podman: localhost/schedr:0.1.0)
#   CONTAINER=podman
set -euo pipefail

if [ "${CLUSTER:-schedr-test}" != schedr-test ]; then
    echo "FAILED: scripts/kind-kill.sh only runs on kind cluster schedr-test (CLUSTER=$CLUSTER)"
    exit 1
fi
if [ -n "${MESH:-}" ]; then
    echo "FAILED: scripts/kind-kill.sh does not take MESH; it runs on plain kind (schedr-test)"
    exit 1
fi
CLUSTER=schedr-test
CONTEXT=kind-$CLUSTER
NAMESPACE=schedr
TOOL=${CONTAINER:-podman}
local_prefix=
[ "$TOOL" = podman ] && local_prefix=localhost/
IMAGE=${IMAGE:-${local_prefix}schedr:0.1.0}
SERVER_IMAGE=${SERVER_IMAGE:-${local_prefix}schedr-server:0.1.0}
WORKER_IMAGE=localhost/schedr-kind-kill:1
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
export KIND_EXPERIMENTAL_PROVIDER=$TOOL

k() { kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
h() { helm --kube-context "$CONTEXT" -n "$NAMESPACE" "$@"; }
say() { printf '\n== %s\n' "$*"; }
fail() {
    echo "FAILED: $*"
    echo "---- pods ----"
    k get pods -o wide 2>/dev/null || true
    echo "---- workers ----"
    if [ -n "${WEB:-}" ]; then
        k exec "$WEB" -c schedr -- wget -qO- -T 5 http://127.0.0.1:8080/api/v1/workers 2>/dev/null || true
        echo
    fi
    exit 1
}

# The API, from inside the pod that serves it (the webserver, or standalone). The body is
# judged by check.py: exit 0 holds, 1 is not yet, 2 means the failure rules broke.
api() { k exec "$1" -c schedr -- wget -qO- -T 5 "http://127.0.0.1:8080$2"; }
judge() {
    local target=$1 path=$2
    shift 2
    local code
    set +e
    JUDGE_OUT=$(api "$target" "$path" | python3 "$WORK/check.py" "$@")
    code=$?
    set -e
    # wget's own failure (the pod is not answering yet) is "not yet".
    if [ "$code" -gt 2 ]; then
        code=1
    fi
    printf '%s' "$JUDGE_OUT"
    return "$code"
}

# Polls judge until it holds. A broken rule stops the script. Writes the checker's line to RESULT.
# Not called inside a command substitution: fail's exit has to leave this script.
wait_for() {
    local what=$1 tries=$2
    shift 2
    local code
    RESULT=
    for _ in $(seq 1 "$tries"); do
        set +e
        RESULT=$(judge "$@")
        code=$?
        set -e
        if [ "$code" -eq 0 ]; then
            return 0
        fi
        if [ "$code" -eq 2 ]; then
            fail "$what: $RESULT"
        fi
        sleep 2
    done
    fail "$what did not happen in time (${RESULT:-no answer})"
}

# While a distributed run finishes, no worker may become lost (the master being down records
# them disconnected, which is not lost). With VICTIM set, that worker may be lost; every
# other one has to stay active.
wait_attempt() {
    local what=$1 tries=$2 path=$3
    shift 3
    local code wcode wstate
    RESULT=
    wstate=
    for _ in $(seq 1 "$tries"); do
        set +e
        if [ -n "${VICTIM:-}" ]; then
            wstate=$(judge "$WEB" /api/v1/workers workers other-active "$VICTIM")
        else
            wstate=$(judge "$WEB" /api/v1/workers workers none-lost)
        fi
        wcode=$?
        set -e
        if [ "$wcode" -eq 2 ]; then
            fail "$what: a worker was lost: $wstate"
        fi
        # An answer that names workers, with the other one not active, is the rule broken.
        # An empty answer is the API not up yet.
        if [ -n "${VICTIM:-}" ] && [ "$wcode" -eq 1 ] && [ -n "$wstate" ]; then
            fail "$what: the other worker is not active: $wstate"
        fi
        set +e
        RESULT=$(judge "$WEB" "$path" "$@")
        code=$?
        set -e
        if [ "$code" -eq 0 ]; then
            return 0
        fi
        if [ "$code" -eq 2 ]; then
            fail "$what: $RESULT"
        fi
        sleep 2
    done
    fail "$what did not happen in time (${RESULT:-no answer}; workers: ${wstate:-unknown})"
}

# Waits until pod NAME is ready and is no longer the pod OLD_UID was.
wait_pod() {
    local name=$1 old=$2 tries=$3
    for _ in $(seq 1 "$tries"); do
        if k get pod "$name" -o json 2>/dev/null | python3 -c '
import json, sys
old = sys.argv[1]
try:
    pod = json.load(sys.stdin)
except json.JSONDecodeError:
    sys.exit(1)
meta = pod["metadata"]
if meta.get("deletionTimestamp") or meta["uid"] == old:
    sys.exit(1)
ready = any(
    c["type"] == "Ready" and c["status"] == "True"
    for c in pod["status"].get("conditions", [])
)
sys.exit(0 if ready else 1)
' "$old"; then
            return 0
        fi
        sleep 2
    done
    fail "pod $name did not come back ready (was $old)"
}

# Waits until the pod selector's ready pod is a different pod from OLD_UID.
wait_replaced() {
    local selector=$1 old=$2 tries=$3
    for _ in $(seq 1 "$tries"); do
        if k get pods -l "$selector" -o json | python3 -c '
import json, sys
old = sys.argv[1]
for pod in json.load(sys.stdin)["items"]:
    meta = pod["metadata"]
    if meta.get("deletionTimestamp") or meta["uid"] == old:
        continue
    ready = any(
        c["type"] == "Ready" and c["status"] == "True"
        for c in pod["status"].get("conditions", [])
    )
    if ready:
        print(meta["name"])
        sys.exit(0)
sys.exit(1)
' "$old"; then
            return 0
        fi
        sleep 2
    done
    fail "no ready replacement for $selector (was $old)"
}

cli() { k exec "$WEB" -c schedr -- schedr "$@" --server http://127.0.0.1:8080; }

# True when every needle occurs in the text. The text is already captured, so nothing is
# piped out of kubectl: under pipefail, `cli | grep -q` can die with SIGPIPE (141).
has_all() {
    local text=$1 needle
    shift
    for needle in "$@"; do
        grep -F -e "$needle" >/dev/null <<<"$text" || return 1
    done
}

# Polls `schedr dags list` until every name is there. One call per poll. Returns 1 and sets
# MISSING_DAG when one never appears, so the caller fails through `fail` (its dump) instead
# of `dags unpause` dying under `set -e`.
wait_dags() {
    local tries=$1
    shift
    local dags="" name
    MISSING_DAG=
    for _ in $(seq 1 "$tries"); do
        dags=$(cli dags list 2>/dev/null || true)
        if has_all "$dags" "$@"; then
            return 0
        fi
        sleep 2
    done
    for name in "$@"; do
        if ! has_all "$dags" "$name"; then
            MISSING_DAG=$name
            return 1
        fi
    done
}

# True when one line of the text contains every needle (a run's id and its state).
on_line() {
    local text=$1 needle
    shift
    needle=$1
    shift
    text=$(grep -F -e "$needle" <<<"$text" || true)
    has_all "$text" "$@"
}

# Polls `schedr runs list DAG` until the line for ID says success.
wait_success() {
    local dag=$1 id=$2 tries=$3
    local out=""
    for _ in $(seq 1 "$tries"); do
        out=$(cli runs list "$dag" 2>/dev/null || true)
        if on_line "$out" "$id" success; then
            return 0
        fi
        sleep 2
    done
    on_line "$out" "$id" success || fail "$dag run $id did not succeed"
}

# [master] lease_timeout. The pod is force-deleted just after, and timed until it is Ready.
# Under this, the attempt has to be WorkerRestarted. At or over it, WorkerLost is the rule.
LEASE_TIMEOUT=60

# Force-deletes pod NAME and waits until that name is Ready as a new pod. Sets BACK_SECS.
kill_pod() {
    local name=$1 old started
    old=$(k get pod "$name" -o jsonpath='{.metadata.uid}')
    [ -n "$old" ] || fail "no pod $name"
    started=$(date +%s)
    k delete pod "$name" --force --grace-period=0 --wait=false
    wait_pod "$name" "$old" 90
    BACK_SECS=$(( $(date +%s) - started ))
    echo "$name was ready again after ${BACK_SECS}s"
}

# WorkerRestarted when the pod was back inside the lease, either exception when it was not.
restart_expect() {
    if [ "$BACK_SECS" -lt "$LEASE_TIMEOUT" ]; then
        echo WorkerRestarted
    else
        echo either
    fi
}

# Printed when the slow return made the attempt WorkerLost, which is then the rule.
note_lost() {
    local who=$1
    case "$RESULT" in
    *WorkerLost*)
        echo "$who was ready after ${BACK_SECS}s, past the ${LEASE_TIMEOUT}s lease, so the attempt is WorkerLost"
        ;;
    esac
}

# Sets RUN_ID. Kept out of a command substitution so a failure exits this script.
trigger() {
    local dag=$1 json
    cli dags unpause "$dag" >/dev/null
    json=$(cli runs trigger "$dag" --json) || fail "runs trigger $dag failed"
    RUN_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["run_id"])' <<<"$json")
    [ -n "$RUN_ID" ] || fail "$dag: no run id ($json)"
    # Decorating a function does not add a task; the call inside `with Dag` does. An empty
    # DAG still triggers, and the attempts document stays empty.
    local body
    body=$(api "$WEB" "/api/v1/runs/$RUN_ID") || fail "$dag: run $RUN_ID is not readable"
    python3 -c '
import json, sys
dag = sys.argv[1]
ids = [item["task_id"] for item in json.load(sys.stdin).get("tasks", [])]
if dag not in ids:
    sys.stderr.write("tasks: " + (", ".join(ids) if ids else "(none)") + "\n")
    sys.exit(1)
' "$dag" <<<"$body" || fail "$dag: run $RUN_ID has no task \`$dag\`"
}

clean_release() {
    h uninstall schedr --wait >/dev/null 2>&1 || true
    k delete deployment,statefulset,service,secret,serviceaccount,poddisruptionbudget,configmap \
        -l app.kubernetes.io/instance=schedr --wait --ignore-not-found >/dev/null
    k delete pvc data-schedr-0 data-schedr-worker-0 data-schedr-worker-1 \
        --ignore-not-found --wait >/dev/null
}

cat > "$WORK/check.py" <<'PY'
"""One API document on stdin. Exit 0 when the condition holds, 1 when it is too early, 2 when
the failure rules broke (a worker lost other than the one just killed, an unexpected
attempt)."""

import json
import sys

ENDED_BAD = {"lost", "failed", "timed_out", "interrupted", "cancelled"}


def load():
    raw = sys.stdin.read()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        sys.exit(1)


def rows():
    return load()["attempts"]


def summary(attempts):
    parts = []
    for item in attempts:
        status = item.get("status") or "running"
        exception = (item.get("error_summary") or {}).get("exception") or ""
        parts.append(f"{status}:{exception}" if exception else status)
    return " ".join(parts)


def workers():
    body = load()["workers"]
    line = " ".join(f"{item['worker_id']}={item['state']}" for item in body)
    lost = [item["worker_id"] for item in body if item["state"] == "lost"]
    return body, line, lost


cmd = sys.argv[1]
if cmd == "running":
    attempts = rows()
    if len(attempts) > 1 or (
        len(attempts) == 1 and attempts[0].get("status") in ENDED_BAD
    ):
        print(summary(attempts))
        sys.exit(2)
    if len(attempts) != 1 or not attempts[0].get("started_at"):
        print(summary(attempts) or "no attempts")
        sys.exit(1)
    print(attempts[0]["attempt_id"], attempts[0].get("worker_id") or "")
elif cmd == "only-id":
    attempts = rows()
    wanted = sys.argv[2]
    if [item["attempt_id"] for item in attempts] != [wanted]:
        print(summary(attempts) or "no attempts")
        sys.exit(2)
    if attempts[0].get("status") in ENDED_BAD:
        print(summary(attempts))
        sys.exit(2)
    print(summary(attempts))
elif cmd == "one-success":
    attempts = rows()
    if len(attempts) == 1 and attempts[0].get("status") == "success":
        print(summary(attempts))
        sys.exit(0)
    if len(attempts) > 1 or any(item.get("status") in ENDED_BAD for item in attempts):
        print(summary(attempts))
        sys.exit(2)
    print(summary(attempts) or "no attempts")
    sys.exit(1)
elif cmd == "retried":
    # argv[2]: WorkerRestarted, or either (that or WorkerLost, when the pod took the lease).
    wanted = sys.argv[2] if len(sys.argv) > 2 else "WorkerRestarted"
    allowed = {"WorkerRestarted", "WorkerLost"} if wanted == "either" else {wanted}
    attempts = rows()
    exception = ""
    if attempts:
        exception = (attempts[0].get("error_summary") or {}).get("exception") or ""
    if (
        len(attempts) == 2
        and attempts[0].get("status") == "lost"
        and exception in allowed
        and attempts[1].get("status") == "success"
    ):
        print(summary(attempts))
        sys.exit(0)
    broken = len(attempts) > 2
    if attempts and attempts[0].get("status") not in (None, "lost"):
        broken = True
    if attempts and attempts[0].get("status") == "lost" and exception and exception not in allowed:
        broken = True
    if len(attempts) == 2 and attempts[1].get("status") not in (None, "success"):
        broken = True
    if broken:
        print(summary(attempts))
        sys.exit(2)
    print(summary(attempts) or "no attempts")
    sys.exit(1)
elif cmd == "workers":
    body, line, lost = workers()
    mode = sys.argv[2]
    if mode == "other-active":
        # The killed worker may be lost. Any other lost worker broke the rule.
        victim = sys.argv[3]
        others = [item for item in body if item["worker_id"] != victim]
        if any(item["state"] == "lost" for item in others):
            print(line)
            sys.exit(2)
        ok = len(others) >= 1 and all(item["state"] == "active" for item in others)
        print(line)
        sys.exit(0 if ok else 1)
    if lost:
        print(line)
        sys.exit(2)
    if mode == "none-lost":
        print(line)
        sys.exit(0)
    if mode == "both-active":
        ok = len(body) == 2 and all(item["state"] == "active" for item in body)
        print(line)
        sys.exit(0 if ok else 1)
    sys.exit(1)
else:
    sys.exit(1)
PY

say "kind cluster $CLUSTER (context $CONTEXT)"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
server=$(kubectl config view -o jsonpath="{.clusters[?(@.name==\"${CONTEXT}\")].cluster.server}")
case "$server" in
https://127.0.0.1:* | https://localhost:* | https://\[::1\]:*) ;;
*) fail "context $CONTEXT has server ${server:-<none>}; this script only talks to local kind" ;;
esac
# A listed cluster can still have its node container stopped (podman shows it Created).
if ! kubectl --context "$CONTEXT" get --raw=/readyz >/dev/null 2>&1; then
    say "starting kind node ${CLUSTER}-control-plane"
    "$TOOL" start "${CLUSTER}-control-plane" \
        || fail "kind cluster $CLUSTER is listed but its node did not start"
    ready=0
    for _ in $(seq 1 30); do
        if kubectl --context "$CONTEXT" get --raw=/readyz >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 2
    done
    [ "$ready" = 1 ] || fail "kind cluster $CLUSTER did not answer after its node was started"
fi
kubectl --context "$CONTEXT" get namespace "$NAMESPACE" >/dev/null 2>&1 \
    || kubectl --context "$CONTEXT" create namespace "$NAMESPACE"

"$TOOL" image inspect "$IMAGE" >/dev/null 2>&1 || fail "no image $IMAGE (run make image)"
"$TOOL" image inspect "$SERVER_IMAGE" >/dev/null 2>&1 \
    || fail "no image $SERVER_IMAGE (run make server-image)"

say "a worker image: $IMAGE with hold (retries=0), again (retries=1) and ping"
cat > "$WORK/hold.py" <<'PYTHON'
import time

from schedr import Dag, task

with Dag("hold"):

    @task(retries=0)
    def hold() -> str:
        time.sleep(90)
        return "held"

    hold()
PYTHON
cat > "$WORK/again.py" <<'PYTHON'
import time
from datetime import timedelta

from schedr import Dag, task

with Dag("again"):

    # The default retry delay is 5 minutes. Zero, so this check sees the retry at once.
    @task(retries=1, retry_delay=timedelta(0))
    def again() -> str:
        time.sleep(30)
        return "again"

    again()
PYTHON
cat > "$WORK/ping.py" <<'PYTHON'
from schedr import CommandOperator, Dag

with Dag("ping"):
    CommandOperator("ping", command="echo pong")
PYTHON
cat > "$WORK/Dockerfile" <<DOCKERFILE
FROM $IMAGE
COPY hold.py again.py ping.py /dags/current/
DOCKERFILE
"$TOOL" build -q -t "$WORKER_IMAGE" "$WORK" >/dev/null
for image in "$WORKER_IMAGE" "$SERVER_IMAGE"; do
    archive="$WORK/$(echo "$image" | tr '/:' '__').tar"
    "$TOOL" save -o "$archive" "$image"
    kind load image-archive --name "$CLUSTER" "$archive"
done

say "Postgres in the namespace, database schedr (kind-distributed.sh's database postgres is left as it is)"
k apply -f - <<'MANIFEST'
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: postgres}
spec:
  serviceName: postgres
  replicas: 1
  selector: {matchLabels: {app: postgres}}
  template:
    metadata: {labels: {app: postgres}}
    spec:
      containers:
        - name: postgres
          image: docker.io/library/postgres:17.6-alpine
          env: [{name: POSTGRES_PASSWORD, value: schedr}]
          ports: [{containerPort: 5432}]
          readinessProbe: {exec: {command: [pg_isready, -U, postgres]}, periodSeconds: 2}
---
apiVersion: v1
kind: Service
metadata: {name: postgres}
spec: {selector: {app: postgres}, ports: [{name: tcp-postgres, port: 5432}]}
MANIFEST
k rollout status statefulset/postgres --timeout=180s
k create secret generic schedr-pg --from-literal=username=postgres --from-literal=password=schedr \
    --dry-run=client -o yaml | k apply -f -

# A registry left by an earlier install would list workers that are already lost.
clean_release
k exec postgres-0 -- psql -U postgres -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS schedr WITH (FORCE)"
k exec postgres-0 -- psql -U postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE schedr"

# Smaller than the chart's defaults, so a kind node can schedule two workers. Same as
# scripts/kind-distributed.sh.
fit=(
    --set persistence.size=1Gi --set worker.persistence.size=1Gi
    --set resources.requests.cpu=100m --set resources.requests.memory=512Mi
    --set resources.limits.memory=1Gi
    --set worker.resources.requests.cpu=100m --set worker.resources.requests.memory=512Mi
    --set worker.resources.limits.memory=1Gi
)
image_set=(
    --set image.repository="${WORKER_IMAGE%:*}" --set image.tag="${WORKER_IMAGE##*:}"
    --set image.pullPolicy=Never
)

say "distributed: two workers, generated certificates"
h upgrade --install schedr "$ROOT/charts/schedr" "${image_set[@]}" "${fit[@]}" \
    --set master.image.repository="${SERVER_IMAGE%:*}" --set master.image.tag="${SERVER_IMAGE##*:}" \
    --set master.image.pullPolicy=Never \
    --set webserver.image.repository="${SERVER_IMAGE%:*}" \
    --set webserver.image.tag="${SERVER_IMAGE##*:}" --set webserver.image.pullPolicy=Never \
    --set database.postgres.host=postgres --set database.postgres.name=schedr \
    --set database.postgres.existingSecret=schedr-pg \
    --set mode=distributed --set tls.mode=app \
    --wait --timeout 8m
k rollout status deployment/schedr-master --timeout=180s
k rollout status deployment/schedr-webserver --timeout=180s
k rollout status statefulset/schedr-worker --timeout=300s

WEB=deploy/schedr-webserver
wait_dags 90 hold again || {
    k logs -l app.kubernetes.io/component=worker --tail=40 || true
    fail "${MISSING_DAG} was not parsed"
}
wait_for "two active workers" 60 "$WEB" /api/v1/workers workers both-active
echo "$RESULT"

MASTER_SELECTOR='app.kubernetes.io/instance=schedr,app.kubernetes.io/component=master'

say "the master killed during hold (retries=0)"
VICTIM=
trigger hold
run=$RUN_ID
wait_attempt "hold to start" 45 "/api/v1/runs/$run/tasks/hold/attempts" running
read -r attempt_id worker_id <<<"$RESULT"
[ -n "$attempt_id" ] && [ -n "$worker_id" ] || fail "hold started without an attempt or a worker"
echo "hold attempt $attempt_id on $worker_id"
old=$(k get pod -l "$MASTER_SELECTOR" -o jsonpath='{.items[0].metadata.uid}')
[ -n "$old" ] || fail "no master pod"
k delete pod -l "$MASTER_SELECTOR" --force --grace-period=0 --wait=false
wait_replaced "$MASTER_SELECTOR" "$old" 90 >/dev/null
wait_attempt "hold still the same attempt" 5 "/api/v1/runs/$run/tasks/hold/attempts" only-id "$attempt_id"
echo "hold after the master came back: $RESULT"
wait_attempt "hold to succeed" 60 "/api/v1/runs/$run/tasks/hold/attempts" one-success
echo "hold finished: $RESULT"
wait_for "both workers active after the master kill" 40 "$WEB" /api/v1/workers workers both-active
echo "$RESULT"
echo "ok: the master kill left one successful attempt; both workers are active"

say "a worker killed during again (retries=1)"
VICTIM=
trigger again
run=$RUN_ID
wait_attempt "again to start" 45 "/api/v1/runs/$run/tasks/again/attempts" running
read -r attempt_id victim <<<"$RESULT"
case "$victim" in
schedr-worker-*) ;;
*) fail "again's worker id \`$victim\` is not a worker pod name" ;;
esac
echo "again attempt $attempt_id on $victim"
kill_pod "$victim"
VICTIM=$victim
wait_attempt "again to be retried" 90 "/api/v1/runs/$run/tasks/again/attempts" retried "$(restart_expect)"
echo "again finished: $RESULT"
note_lost "$victim"
wait_for "the other worker still active after the worker kill" 40 \
    "$WEB" /api/v1/workers workers other-active "$victim"
echo "$RESULT"
VICTIM=
echo "ok: the worker kill retried and succeeded; the other worker stayed active"

say "standalone on SQLite, then its pod killed during again"
clean_release
h upgrade --install schedr "$ROOT/charts/schedr" "${image_set[@]}" "${fit[@]}" \
    --set mode=standalone --wait --timeout 6m
k rollout status statefulset/schedr --timeout=180s
WEB=schedr-0
wait_dags 90 ping again || fail "${MISSING_DAG} was not parsed"
trigger ping
ping=$RUN_ID
wait_success ping "$ping" 45
echo "ok: ping $ping succeeded"
trigger again
run=$RUN_ID
wait_for "again to start on standalone" 45 "$WEB" "/api/v1/runs/$run/tasks/again/attempts" running
read -r attempt_id worker_id <<<"$RESULT"
[ "$worker_id" = schedr-0 ] || fail "standalone's worker id is \`$worker_id\`, expected schedr-0"
echo "again attempt $attempt_id on $worker_id"
kill_pod schedr-0
for _ in $(seq 1 30); do
    if api "$WEB" /api/v1/workers >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
out=$(cli runs list ping 2>/dev/null || true)
on_line "$out" "$ping" success || fail "ping is gone after the pod was killed (the volume)"
wait_for "again to be retried on standalone" 90 "$WEB" "/api/v1/runs/$run/tasks/again/attempts" retried "$(restart_expect)"
echo "$RESULT"
note_lost schedr-0
echo "ok: standalone's volume kept ping; again was retried and succeeded"

say "done: master kill, worker kill, standalone kill, on $CONTEXT"
