#!/usr/bin/env bash
# Installs the chart's mode distributed on a LOCAL kind cluster, moving there from standalone on
# the same Postgres, and checks it: the history kept, two workers registered over mTLS with the
# chart's generated certificates, a DAG run, a task's log read through the webserver from its
# worker, the certificates kept by an upgrade, and a worker stopped while its task runs (the
# task finishes in the drain, one attempt). Every kubectl and helm call names the kind
# cluster's context explicitly: it never touches another cluster. The context's server has
# to be loopback. Shared pieces live in scripts/kind-lib.sh.
#
# Usage: scripts/kind-distributed.sh   (the images `make image` and `make server-image` built)
#   CLUSTER=schedr-test  the kind cluster (created if missing, kept afterwards)
#   IMAGE, SERVER_IMAGE  the images, as the container tool names them (podman:
#                        localhost/schedr:0.1.0, localhost/schedr-server:0.1.0)
#   CONTAINER=podman
#   MESH=1  on a cluster with Istio (scripts/kind-mesh.sh's, CLUSTER=schedr-mesh): tls.mode mesh,
#           an account per role and the internal links allowed by account only
#           (sameNamespace false)
set -euo pipefail
CLUSTER=${CLUSTER:-schedr-test}
CONTEXT=kind-$CLUSTER
NAMESPACE=schedr
MESH=${MESH:-}
WORKER_IMAGE=localhost/schedr-kind-distributed:1
ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=kind-lib.sh
source "$ROOT/scripts/kind-lib.sh"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

say "kind cluster $CLUSTER (context $CONTEXT)${MESH:+, in the mesh}"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
require_local_context
start_kind_node
kubectl --context "$CONTEXT" get namespace "$NAMESPACE" >/dev/null 2>&1 \
    || kubectl --context "$CONTEXT" create namespace "$NAMESPACE"

say "images: the workers' ($IMAGE with the example DAG and a slow one), the server's"
cp "$ROOT/examples/daily_report.py" "$WORK/"
cat > "$WORK/slow.py" <<'PYTHON'
import time

from schedr import Dag, task

with Dag("slow"):

    @task
    def nap() -> str:
        time.sleep(20)
        return "rested"

    nap()
PYTHON
cat > "$WORK/Dockerfile" <<DOCKERFILE
FROM $IMAGE
COPY daily_report.py slow.py /dags/current/
DOCKERFILE
"$TOOL" build -q -t "$WORKER_IMAGE" "$WORK" >/dev/null
load_images "$WORKER_IMAGE" "$SERVER_IMAGE"

say "Postgres in the namespace"
apply_postgres

values=(
    --set image.repository="${WORKER_IMAGE%:*}" --set image.tag="${WORKER_IMAGE##*:}"
    --set image.pullPolicy=Never
    --set master.image.repository="${SERVER_IMAGE%:*}" --set master.image.tag="${SERVER_IMAGE##*:}"
    --set master.image.pullPolicy=Never
    --set webserver.image.repository="${SERVER_IMAGE%:*}" --set webserver.image.tag="${SERVER_IMAGE##*:}"
    --set webserver.image.pullPolicy=Never
    --set database.postgres.host=postgres --set database.postgres.name=postgres
    --set database.postgres.existingSecret=schedr-pg
    --set terminationGracePeriodSeconds=60
    "${fit[@]}"
)
if [ -n "$MESH" ]; then
    values+=(
        --set istio.enabled=true --set migration=onStart --set serviceAccount.create=true
        --set serviceAccount.name= --set istio.authorizationPolicy.enabled=true
        --set istio.authorizationPolicy.sameNamespace=false
        --set istio.authorizationPolicy.api.requireJwt=false
    )
fi

say "standalone first, on Postgres, with a run of its own"
# From a clean release: `helm uninstall` removes only what the release's last record lists, so
# a failed upgrade's objects (the other mode's pods) stay; whatever carries the release's label
# goes too (Postgres doesn't). Postgres and its history stay.
h uninstall schedr --wait >/dev/null 2>&1 || true
kinds=deployment,statefulset,service,secret,serviceaccount,poddisruptionbudget,configmap
if [ -n "$MESH" ]; then
    kinds=$kinds,authorizationpolicies.security.istio.io,requestauthentications.security.istio.io
    kinds=$kinds,peerauthentications.security.istio.io,virtualservices.networking.istio.io
    kinds=$kinds,destinationrules.networking.istio.io
fi
k delete "$kinds" -l app.kubernetes.io/instance=schedr --wait --ignore-not-found >/dev/null
k delete pvc data-schedr-0 data-schedr-worker-0 data-schedr-worker-1 --ignore-not-found --wait >/dev/null
h upgrade --install schedr "$ROOT/charts/schedr" "${values[@]}" --set mode=standalone \
    --wait --timeout 6m
k rollout restart statefulset/schedr
k rollout status statefulset/schedr --timeout=180s
standalone_cli() { k exec schedr-0 -c schedr -- schedr "$@" --server http://127.0.0.1:8080; }
for _ in $(seq 1 90); do standalone_cli dags list 2>/dev/null | grep -q daily_report && break; sleep 2; done
standalone_cli dags unpause daily_report
before=$(standalone_cli runs trigger daily_report | awk '/^triggered run/ {print $3}')
for _ in $(seq 1 90); do
    standalone_cli runs list daily_report 2>/dev/null | grep "$before" | grep -q success && break
    sleep 2
done
standalone_cli runs list daily_report | grep "$before" | grep -q success \
    || fail "standalone's run didn't succeed"

tls_text="app, generated"; [ -n "$MESH" ] && tls_text=mesh
say "moved to distributed: the same release and Postgres, mode distributed (TLS: $tls_text)"
tls=(--set tls.mode=app)
[ -n "$MESH" ] && tls=(--set tls.mode=mesh)
h upgrade schedr "$ROOT/charts/schedr" "${values[@]}" "${tls[@]}" --set mode=distributed \
    --wait --timeout 8m
k rollout status deployment/schedr-master --timeout=180s
k rollout status deployment/schedr-webserver --timeout=180s
k rollout status statefulset/schedr-worker --timeout=300s
cli() { k exec deploy/schedr-webserver -c schedr -- schedr "$@" --server http://127.0.0.1:8080; }
cli runs list daily_report | grep "$before" | grep -q success || fail "standalone's history is gone"
echo "ok: standalone's run is still there"
for _ in $(seq 1 60); do
    [ "$(cli workers list 2>/dev/null | grep -c active)" -ge 2 ] && break
    sleep 2
done
cli workers list
[ "$(cli workers list | grep -c active)" -ge 2 ] || fail "not two active workers"

say "a run across the workers, and a task's log through the webserver"
run=$(cli runs trigger daily_report | awk '/^triggered run/ {print $3}')
for _ in $(seq 1 90); do cli runs list daily_report | grep "$run" | grep -q success && break; sleep 2; done
cli runs list daily_report | grep "$run" | grep -q success || { k logs deploy/schedr-master --tail=40; fail "the run didn't succeed"; }
cli tasks logs "$run" build | grep -q building || fail "the task's log isn't readable through the webserver"
echo "ok: the run succeeded; the build task's log read from its worker"

if [ -z "$MESH" ]; then
    say "the generated certificates kept by an upgrade"
    cert=$(k get secret schedr-master-tls -o jsonpath='{.data.tls\.crt}')
    h upgrade schedr "$ROOT/charts/schedr" "${values[@]}" "${tls[@]}" --set mode=distributed \
        --wait --timeout 5m
    [ "$(k get secret schedr-master-tls -o jsonpath='{.data.tls\.crt}')" = "$cert" ] \
        || fail "the upgrade made new certificates"
    echo "ok: the same certificates after an upgrade"
fi

say "a worker stopped while its task runs: the task finishes in the drain"
cli dags unpause slow
slow=$(cli runs trigger slow | awk '/^triggered run/ {print $3}')
worker=
for _ in $(seq 1 60); do
    worker=$(k exec deploy/schedr-webserver -c schedr -- wget -qO- \
        "http://127.0.0.1:8080/api/v1/runs/$slow/tasks/nap/attempts" 2>/dev/null \
        | python3 -c 'import json, sys; a = json.load(sys.stdin)["attempts"]; print(a[0]["worker_id"] if a else "")' \
        2>/dev/null || true)
    [ -n "$worker" ] && break
    sleep 1
done
[ -n "$worker" ] || fail "the slow task didn't start"
sleep 3
k delete pod "$worker" --wait=true
for _ in $(seq 1 60); do cli runs list slow | grep "$slow" | grep -q success && break; sleep 2; done
attempts=$(k exec deploy/schedr-webserver -c schedr -- wget -qO- \
    "http://127.0.0.1:8080/api/v1/runs/$slow/tasks/nap/attempts")
echo "$attempts" | python3 -c '
import json, sys
attempts = json.load(sys.stdin)["attempts"]
print([(a["worker_id"], a["status"]) for a in attempts])
assert [a["status"] for a in attempts] == ["success"], "expected one successful attempt"'
k rollout status statefulset/schedr-worker --timeout=180s

say "done: distributed after standalone, on the same Postgres${MESH:+, in the mesh}"
