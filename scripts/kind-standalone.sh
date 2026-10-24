#!/usr/bin/env bash
# Installs the chart's standalone mode on a LOCAL kind cluster and runs a DAG through it: first
# on SQLite (its volume), then switched to Postgres (database.postgres: the host in values, the
# user and password from a Secret), then on Postgres with the whole URL in a Secret; then DAGs
# from a folder (charts/schedr/dags-values.sh) through `helm upgrade` without a restart, and
# DAGs from git-sync (a git server in the cluster). Every kubectl and helm call
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

# run_a_dag [dag]: waits for the DAG (daily_report), unpauses it, runs it and waits for success.
run_a_dag() {
    local dag=${1:-daily_report}
    k rollout status statefulset/schedr --timeout=180s
    for _ in $(seq 1 90); do
        cli dags list 2>/dev/null | grep -q "$dag" && break
        sleep 2
    done
    cli dags unpause "$dag"
    # The run this call triggers (on the same Postgres, earlier runs succeeded already).
    local run
    run=$(cli runs trigger "$dag" | awk '/^triggered run/ {print $3}')
    [ -n "$run" ] || { echo "no run ID from runs trigger"; return 1; }
    for _ in $(seq 1 90); do
        if cli runs list "$dag" 2>/dev/null | grep "$run" | grep -q success; then
            cli runs list "$dag"
            return 0
        fi
        sleep 2
    done
    cli runs list "$dag" || true
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
k create secret generic schedr-pg --from-literal=username=postgres --from-literal=password=schedr \
    --dry-run=client -o yaml | k apply -f -
k create secret generic schedr-db \
    --from-literal=url=postgres://postgres:schedr@postgres:5432/postgres \
    --dry-run=client -o yaml | k apply -f -

say "switched to Postgres, user and password from a Secret (a fresh start: SQLite's history stays behind)"
h upgrade schedr "$ROOT/charts/schedr" "${chart_values[@]}" --set database.postgres.host=postgres \
    --set database.postgres.name=postgres --set database.postgres.existingSecret=schedr-pg \
    --wait --timeout 5m
k rollout status statefulset/schedr --timeout=180s
run_a_dag

say "standalone on Postgres, the whole URL in a Secret"
h upgrade schedr "$ROOT/charts/schedr" "${chart_values[@]}" \
    --set database.existingSecret=schedr-db --wait --timeout 5m
k rollout restart statefulset/schedr
run_a_dag
k exec schedr-0 -c schedr -- sh -c 'wget -qO- http://127.0.0.1:8090/metrics | grep "^schedr_master_leading"'

# A DAG of its own ID: what each phase below delivers.
a_dag() {
    printf 'from schedr import CommandOperator, Dag\n\nwith Dag("%s"):\n    CommandOperator("hello", command="echo hello")\n' "$1"
}

say "DAGs from a folder: dags-values.sh, then helm upgrade, without restarting schedr"
mkdir -p "$WORK/folder"
a_dag packed > "$WORK/folder/packed.py"
printf 'X = 1\n' > "$WORK/folder/helpers.py"
"$ROOT/charts/schedr/dags-values.sh" "$WORK/folder" > "$WORK/dags-values.yaml"
h upgrade schedr "$ROOT/charts/schedr" "${chart_values[@]}" \
    --set database.existingSecret=schedr-db -f "$WORK/dags-values.yaml" --wait --timeout 5m
# The ConfigMap's volume is new to the pod spec this once (a restart); files added later
# (a_dag packed_again, a name sorting anywhere) change only the ConfigMap.
run_a_dag packed
uid=$(k get pod schedr-0 -o jsonpath='{.metadata.uid}')
a_dag again_packed > "$WORK/folder/again_packed.py"
"$ROOT/charts/schedr/dags-values.sh" "$WORK/folder" > "$WORK/dags-values.yaml"
h upgrade schedr "$ROOT/charts/schedr" "${chart_values[@]}" \
    --set database.existingSecret=schedr-db -f "$WORK/dags-values.yaml" --wait --timeout 5m
run_a_dag again_packed
[ "$(k get pod schedr-0 -o jsonpath='{.metadata.uid}')" = "$uid" ] \
    || { echo "the pod was replaced for a DAG change"; exit 1; }
echo "ok: a new DAG file reached schedr without a restart"

say "DAGs from git-sync: a git server in the cluster"
k create configmap git-content --from-file=synced.py=<(a_dag synced) \
    --dry-run=client -o yaml | k apply -f -
k apply -f - <<'MANIFEST'
apiVersion: apps/v1
kind: Deployment
metadata: {name: git-server}
spec:
  replicas: 1
  selector: {matchLabels: {app: git-server}}
  template:
    metadata: {labels: {app: git-server}}
    spec:
      containers:
        - name: git
          image: docker.io/library/alpine:3.24
          command: [sh, -c]
          args:
            - >-
              apk add --no-cache git git-daemon &&
              git init -q -b main /work && cp /content/synced.py /work/ &&
              git -C /work add . && git -C /work -c user.name=t -c user.email=t@t commit -qm dags &&
              git clone -q --bare /work /srv/dags.git &&
              exec git daemon --export-all --base-path=/srv --reuseaddr --port=9418 /srv
          ports: [{containerPort: 9418}]
          readinessProbe: {tcpSocket: {port: 9418}, periodSeconds: 2}
          volumeMounts: [{name: content, mountPath: /content}]
      volumes: [{name: content, configMap: {name: git-content}}]
---
apiVersion: v1
kind: Service
metadata: {name: git-server}
spec: {selector: {app: git-server}, ports: [{name: tcp-git, port: 9418}]}
MANIFEST
k rollout status deployment/git-server --timeout=180s
h upgrade schedr "$ROOT/charts/schedr" "${chart_values[@]}" \
    --set database.existingSecret=schedr-db -f "$WORK/dags-values.yaml" \
    --set gitSync.enabled=true --set gitSync.repo=git://git-server:9418/dags.git \
    --set gitSync.period=5s --wait --timeout 5m
run_a_dag synced
run_a_dag packed

say "done: the chart's standalone mode ran DAGs on SQLite, on Postgres, from a folder and from git"
