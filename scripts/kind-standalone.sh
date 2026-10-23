#!/usr/bin/env bash
# Installs the chart's standalone mode on a LOCAL kind cluster and runs a DAG through it: first
# on SQLite (its volume), then on Postgres (a Secret with the URL). Every kubectl and helm call
# names the kind cluster's context explicitly: it never touches another cluster.
#
# Usage: scripts/kind-standalone.sh            (the base image `make image` built must exist)
#   CLUSTER=schedr-test  the kind cluster (created if missing, kept afterwards; delete it with
#                        `kind delete cluster --name schedr-test`)
#   IMAGE=schedr:0.1.0   the base image;  CONTAINER=podman  the container tool
set -euo pipefail
CLUSTER=${CLUSTER:-schedr-test}
CONTEXT=kind-$CLUSTER
NAMESPACE=schedr
IMAGE=${IMAGE:-schedr:0.1.0}
TOOL=${CONTAINER:-podman}
TEST_IMAGE=localhost/schedr-kind-test:1
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
export KIND_EXPERIMENTAL_PROVIDER=$TOOL

k() { kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
h() { helm --kube-context "$CONTEXT" -n "$NAMESPACE" "$@"; }
say() { printf '\n== %s\n' "$*"; }

say "kind cluster $CLUSTER (context $CONTEXT)"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
kubectl --context "$CONTEXT" get namespace "$NAMESPACE" >/dev/null 2>&1 \
    || kubectl --context "$CONTEXT" create namespace "$NAMESPACE"

say "a test image: $IMAGE with the example DAG baked in"
cp "$ROOT/examples/daily_report.py" "$WORK/"
cat > "$WORK/Dockerfile" <<DOCKERFILE
FROM $IMAGE
COPY daily_report.py /dags/current/daily_report.py
DOCKERFILE
"$TOOL" build -q -t "$TEST_IMAGE" "$WORK" >/dev/null
"$TOOL" save -o "$WORK/image.tar" "$TEST_IMAGE"
kind load image-archive --name "$CLUSTER" "$WORK/image.tar"

# Runs the CLI in the pod, against its own API.
cli() { k exec schedr-0 -c schedr -- schedr "$@" --server http://127.0.0.1:8080; }

run_a_dag() {
    k rollout status statefulset/schedr --timeout=180s
    for _ in $(seq 1 60); do
        cli dags list 2>/dev/null | grep -q daily_report && break
        sleep 2
    done
    cli dags unpause daily_report
    cli runs trigger daily_report
    for _ in $(seq 1 90); do
        if cli runs list daily_report 2>/dev/null | grep -q success; then
            cli runs list daily_report
            return 0
        fi
        sleep 2
    done
    cli runs list daily_report || true
    k logs schedr-0 -c schedr --tail=50 || true
    echo "the run didn't succeed"; return 1
}

chart_values=(
    --set image.repository="${TEST_IMAGE%:*}" --set image.tag="${TEST_IMAGE##*:}"
    --set image.pullPolicy=Never
    --set persistence.size=1Gi --set terminationGracePeriodSeconds=30
    --set resources.requests.cpu=100m --set resources.requests.memory=512Mi
    --set resources.limits.memory=1Gi
)

say "standalone on SQLite"
h uninstall schedr --wait >/dev/null 2>&1 || true
k delete pvc --all --wait >/dev/null 2>&1 || true
h install schedr "$ROOT/charts/schedr" "${chart_values[@]}" --wait --timeout 5m
run_a_dag

say "Postgres in the cluster"
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
spec: {selector: {app: postgres}, ports: [{port: 5432}]}
MANIFEST
k rollout status statefulset/postgres --timeout=180s
k create secret generic schedr-db \
    --from-literal=url=postgres://postgres:schedr@postgres:5432/postgres \
    --dry-run=client -o yaml | k apply -f -

say "standalone on Postgres (a fresh start: phase 1's SQLite history stays behind)"
h upgrade schedr "$ROOT/charts/schedr" "${chart_values[@]}" \
    --set database.existingSecret=schedr-db --wait --timeout 5m
k rollout restart statefulset/schedr
run_a_dag
k exec schedr-0 -c schedr -- sh -c 'wget -qO- http://127.0.0.1:8090/metrics | grep "^schedr_master_leading"'

say "done: the chart's standalone mode ran a DAG on SQLite and on Postgres"
