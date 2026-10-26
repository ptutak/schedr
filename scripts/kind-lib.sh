#!/usr/bin/env bash
# Shared by scripts/kind-distributed.sh, scripts/kind-kill.sh, scripts/kind-standalone.sh
# and scripts/kind-mesh.sh (sourced, not run).
#
# The caller sets CLUSTER, CONTEXT and NAMESPACE, and may set CONTAINER, IMAGE and
# SERVER_IMAGE, before sourcing. podman names a local image localhost/<name>; docker keeps
# the bare name. The default IMAGE and SERVER_IMAGE get that prefix only for podman.
# kind-standalone.sh and kind-mesh.sh set IMAGE themselves (the unqualified name).

# shellcheck disable=SC2034 # fit, TOOL, IMAGE and SERVER_IMAGE are for the caller.
# shellcheck disable=SC2154 # CLUSTER, CONTEXT, NAMESPACE and WORK come from the caller.

# Drop a WEB from the environment. kind-kill.sh sets it after sourcing; the other scripts never do.
WEB=

# podman names local images localhost/...; docker, docker.io/library/... (the bare name).
TOOL=${CONTAINER:-podman}
local_prefix=
if [ "$TOOL" = podman ]; then
    local_prefix=localhost/
fi
export KIND_EXPERIMENTAL_PROVIDER=$TOOL
IMAGE=${IMAGE:-${local_prefix}schedr:0.1.0}
SERVER_IMAGE=${SERVER_IMAGE:-${local_prefix}schedr-server:0.1.0}

k() { kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
h() { helm --kube-context "$CONTEXT" -n "$NAMESPACE" "$@"; }
say() { printf '\n== %s\n' "$*"; }

# The pods, and the worker list when WEB is set (kind-kill.sh, after sourcing). WEB is cleared
# above, so a value from the environment is not a worker pod.
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

# The context's server has to be loopback: a kubeconfig that points it elsewhere would make
# every later kubectl and helm call talk to that cluster.
require_local_context() {
    local server
    server=$(kubectl config view -o jsonpath="{.clusters[?(@.name==\"${CONTEXT}\")].cluster.server}")
    case "$server" in
    https://127.0.0.1:* | https://localhost:* | https://\[::1\]:*) ;;
    *)
        # No pod dump: that kubectl would talk to the cluster this refuses.
        echo "FAILED: context $CONTEXT has server ${server:-<none>}; this script only talks to local kind"
        exit 1
        ;;
    esac
}

# A listed cluster can still have its node container stopped (podman shows it Created).
start_kind_node() {
    local ready
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
}

# One archive per image under $WORK; `/` and `:` in the name become `_`.
load_images() {
    local image archive
    for image in "$@"; do
        archive="$WORK/$(echo "$image" | tr '/:' '__').tar"
        "$TOOL" save -o "$archive" "$image"
        kind load image-archive --name "$CLUSTER" "$archive"
    done
}

# Postgres in the namespace, and the Secret schedr-pg (username postgres, password schedr).
# The port is named tcp-postgres so Istio treats it as TCP. The database is the caller's:
# nothing here sets POSTGRES_DB, so the image's default database is postgres.
apply_postgres() {
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
          image: docker.io/library/postgres:17.6-alpine@sha256:ef257d85f76e48da1c64832459b59fcaba1a4dac97bf5d7450c77753542eee94
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
}

# Smaller than the chart's defaults, so one kind node can schedule two workers.
# terminationGracePeriodSeconds is not here: the scripts don't agree (30, 60, or the chart's
# default) and a worker's drain depends on it.
fit=(
    --set persistence.size=1Gi --set worker.persistence.size=1Gi
    --set resources.requests.cpu=100m --set resources.requests.memory=512Mi
    --set resources.limits.memory=1Gi
    --set worker.resources.requests.cpu=100m --set worker.resources.requests.memory=512Mi
    --set worker.resources.limits.memory=1Gi
)
