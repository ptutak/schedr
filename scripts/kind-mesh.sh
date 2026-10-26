#!/usr/bin/env bash
# Installs the chart's standalone mode inside an Istio mesh on a LOCAL kind cluster (outbound
# traffic REGISTRY_ONLY, as on the target cluster) with every Istio object the chart has, and
# checks what the mesh allows: Swagger UI and openapi.json for everyone through the gateway,
# the API only with a valid token carrying the required claim, the CLI with `--token`, a DAG
# run on Postgres through the sidecar (migrated on start), and a drain whose task finishes
# after SIGTERM with its result still reaching the database. Every kubectl, helm and istioctl
# call names the kind cluster's context explicitly: it never touches another cluster. The
# context's server has to be loopback. Shared pieces live in scripts/kind-lib.sh.
#
# Usage: scripts/kind-mesh.sh     (the base image `make image` built must exist, and a schedr
#                                  binary for the CLI on this machine)
#   CLUSTER=schedr-mesh  the kind cluster (created if missing, kept afterwards; delete it with
#                        `kind delete cluster --name schedr-mesh`)
#   IMAGE=schedr:0.1.0   the base image;  CONTAINER=podman  the container tool
#   SCHEDR_BIN=target/debug/schedr  the CLI;  ISTIO=1.31.1  istioctl, kept in target/tools
#   SIDECARS=native  Istio's default here; classic: the sidecar as a plain container (as older
#                    Istio and OpenShift Service Mesh 2 run it), started after init containers
#                    and stopped with the pod: what migration onStart and the chart's proxy
#                    settings are for
set -euo pipefail
CLUSTER=${CLUSTER:-schedr-mesh}
CONTEXT=kind-$CLUSTER
NAMESPACE=schedr
# Unqualified, so this stays schedr:0.1.0. kind-lib.sh would add podman's localhost/ prefix.
IMAGE=${IMAGE:-schedr:0.1.0}
ISTIO=${ISTIO:-1.31.1}
SIDECARS=${SIDECARS:-native}
case $SIDECARS in native) NATIVE=true ;; classic) NATIVE=false ;; *) echo "SIDECARS: native or classic"; exit 2 ;; esac
TEST_IMAGE=localhost/schedr-kind-mesh:1
ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=kind-lib.sh
source "$ROOT/scripts/kind-lib.sh"
SCHEDR_BIN=${SCHEDR_BIN:-$ROOT/target/debug/schedr}
ISTIOCTL=$ROOT/target/tools/istioctl-$ISTIO
GATEWAY_PORT=18080
# Served under a path prefix, rewritten away by the VirtualService, as on the target cluster.
PREFIX=/service/schedr
WORK=$(mktemp -d)
FORWARD=
trap '[ -n "$FORWARD" ] && kill "$FORWARD" 2>/dev/null; rm -rf "$WORK"' EXIT

# Not fail(): that dumps pods, and this runs before the context is known to be local.
[ -x "$SCHEDR_BIN" ] || { echo "FAILED: no schedr binary at $SCHEDR_BIN (cargo build, or set SCHEDR_BIN)"; exit 1; }

say "istioctl $ISTIO"
if [ ! -x "$ISTIOCTL" ]; then
    mkdir -p "$ROOT/target/tools"
    curl -sSfL "https://github.com/istio/istio/releases/download/$ISTIO/istioctl-$ISTIO-linux-amd64.tar.gz" \
        | tar -xz -C "$WORK"
    mv "$WORK/istioctl" "$ISTIOCTL"
fi

say "kind cluster $CLUSTER (context $CONTEXT) with Istio, outbound REGISTRY_ONLY, $SIDECARS sidecars"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
require_local_context
start_kind_node
"$ISTIOCTL" install --context "$CONTEXT" -y --set profile=default \
    --set meshConfig.outboundTrafficPolicy.mode=REGISTRY_ONLY \
    --set values.pilot.env.ENABLE_NATIVE_SIDECARS=$NATIVE
kubectl --context "$CONTEXT" get namespace "$NAMESPACE" >/dev/null 2>&1 \
    || kubectl --context "$CONTEXT" create namespace "$NAMESPACE"

say "a test image: $IMAGE with the example DAG and a slow one baked in"
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
"$TOOL" build -q -t "$TEST_IMAGE" "$WORK" >/dev/null
load_images "$TEST_IMAGE"

say "Postgres in the namespace (outside the mesh), an existing ServiceAccount, a Gateway"
apply_postgres
k apply -f - <<'MANIFEST'
apiVersion: v1
kind: ServiceAccount
metadata: {name: mesh-workload}
---
apiVersion: networking.istio.io/v1
kind: Gateway
metadata: {name: main}
spec:
  selector: {istio: ingressgateway}
  servers:
    - port: {number: 80, name: http, protocol: HTTP}
      hosts: ["*"]
MANIFEST

say "a test issuer: an RSA key, its JWKS, and tokens"
openssl genrsa -out "$WORK/issuer.pem" 2048 2>/dev/null
openssl genrsa -out "$WORK/other.pem" 2048 2>/dev/null
ISSUER=https://issuer.mesh.test
JWKS=$(openssl rsa -in "$WORK/issuer.pem" -noout -modulus | cut -d= -f2 | python3 -c '
import base64, json, sys
n = bytes.fromhex(sys.stdin.read().strip())
b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
print(json.dumps({"keys": [{"kty": "RSA", "alg": "RS256", "use": "sig", "kid": "test",
                            "n": b64(n), "e": b64((65537).to_bytes(3, "big"))}]}))')
# token <key file> <groups as JSON>: an RS256 token from ISSUER, valid for an hour.
token() {
    local b64='import base64, sys; print(base64.urlsafe_b64encode(sys.stdin.buffer.read()).rstrip(b"=").decode())'
    local now; now=$(date +%s)
    local header payload signature
    header=$(printf '{"alg":"RS256","kid":"test","typ":"JWT"}' | python3 -c "$b64")
    payload=$(printf '{"iss":"%s","sub":"tester","iat":%s,"exp":%s,"groups":%s}' \
        "$ISSUER" "$now" "$((now + 3600))" "$2" | python3 -c "$b64")
    signature=$(printf '%s.%s' "$header" "$payload" | openssl dgst -sha256 -sign "$1" | python3 -c "$b64")
    printf '%s.%s.%s' "$header" "$payload" "$signature"
}
GOOD=$(token "$WORK/issuer.pem" '["schedr-users"]')
NO_GROUP=$(token "$WORK/issuer.pem" '["others"]')
FORGED=$(token "$WORK/other.pem" '["schedr-users"]')

say "the chart: standalone on Postgres, every Istio object, migrated on start"
# terminationGracePeriodSeconds stays 60: fit does not set it, and the scripts disagree.
# Sizes (persistence, resources) come from fit: its --set wins over the values below.
h upgrade --install schedr "$ROOT/charts/schedr" --wait --timeout 6m "${fit[@]}" -f - <<VALUES
image: {repository: "${TEST_IMAGE%:*}", tag: "${TEST_IMAGE##*:}", pullPolicy: Never}
database: {postgres: {host: postgres, name: postgres, existingSecret: schedr-pg}}
migration: onStart
serviceAccount: {create: false, name: mesh-workload}
terminationGracePeriodSeconds: 60
istio:
  enabled: true
  virtualService: {enabled: true, gateways: [$NAMESPACE/main], hosts: ["*"], prefix: $PREFIX/}
  requestAuthentication: {enabled: true, issuer: $ISSUER, jwks: '$JWKS'}
  authorizationPolicy:
    enabled: true
    api: {claims: {groups: [schedr-users]}}
  peerAuthentication: {enabled: true}
VALUES
# Injected as the mesh does it now (the cluster may have run with the other kind of sidecar).
k rollout restart statefulset/schedr
k rollout status statefulset/schedr --timeout=180s
# A classic sidecar among the containers, a native one among the init containers.
if [ "$SIDECARS" = classic ]; then
    k get pod schedr-0 -o jsonpath='{.spec.containers[*].name}' | grep -qw istio-proxy \
        || fail "schedr's pod has no classic sidecar"
else
    k get pod schedr-0 -o jsonpath='{.spec.initContainers[*].name}' | grep -qw istio-proxy \
        || fail "schedr's pod has no native sidecar"
fi

say "the gateway on 127.0.0.1:$GATEWAY_PORT"
kubectl --context "$CONTEXT" -n istio-system port-forward svc/istio-ingressgateway \
    "$GATEWAY_PORT:80" >/dev/null 2>&1 &
FORWARD=$!
GATEWAY=http://127.0.0.1:$GATEWAY_PORT$PREFIX
# status <path> [token]: the HTTP status through the gateway.
status() {
    curl -s -o /dev/null -w '%{http_code}' ${2:+-H "Authorization: Bearer $2"} "$GATEWAY$1"
}
for _ in $(seq 1 30); do [ "$(status /api/v1/openapi.json)" = 200 ] && break; sleep 2; done
expect() {
    local got; got=$(status "$2" "${3:-}")
    [ "$got" = "$1" ] || fail "$2 ${4:-}: $got, expected $1"
    echo "ok: $2 ${4:-} -> $got"
}
expect 200 /api/v1/openapi.json "" "without a token"
expect 200 /api/v1/docs/ "" "without a token"
expect 303 /api/v1/docs "" "without a token"
expect 403 /api/v1/dags "" "without a token"
expect 401 /api/v1/dags "$FORGED" "with a forged token"
expect 403 /api/v1/dags "$NO_GROUP" "without the group"
expect 200 /api/v1/dags "$GOOD" "with a valid token"
curl -sf "$GATEWAY/api/v1/openapi.json" | python3 -c '
import json, sys
servers = json.load(sys.stdin).get("servers")
assert servers == [{"url": sys.argv[1]}], servers
print("ok: the description names the prefix as its server")' "$PREFIX"

# The CLI on this machine, through the gateway with its token.
cli() { "$SCHEDR_BIN" "$@" --server "$GATEWAY" --token "$GOOD"; }
api() { curl -sf -H "Authorization: Bearer $GOOD" "$GATEWAY$1"; }

say "a DAG through the CLI with --token"
for _ in $(seq 1 60); do
    api /api/v1/dags | grep -q '"slow"' && api /api/v1/dags | grep -q daily_report && break
    sleep 2
done
cli dags unpause daily_report
cli runs trigger daily_report
for _ in $(seq 1 90); do
    cli runs list daily_report 2>/dev/null | grep -q success && break
    sleep 2
done
cli runs list daily_report | grep -q success || { k logs schedr-0 -c schedr --tail=50; fail "the run didn't succeed"; }
cli runs list daily_report

say "a drain under the mesh: SIGTERM while a task runs; its result still reaches Postgres"
cli dags unpause slow
RUN=$("$SCHEDR_BIN" runs trigger slow --server "$GATEWAY" --token "$GOOD" --json \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["result"]["run_id"])')
attempts() { api "/api/v1/runs/$RUN/tasks/nap/attempts"; }
for _ in $(seq 1 60); do attempts 2>/dev/null | grep -q '"try_number"' && break; sleep 1; done
sleep 3
k delete pod schedr-0 --wait=true
k rollout status statefulset/schedr --timeout=180s
for _ in $(seq 1 60); do
    api "/api/v1/runs/$RUN" 2>/dev/null \
        | python3 -c 'import json, sys; sys.exit(json.load(sys.stdin)["run"]["state"] != "success")' \
            2>/dev/null && break
    sleep 2
done
attempts | python3 -c '
import json, sys
attempts = json.load(sys.stdin)["attempts"]
print(attempts)
assert [a["status"] for a in attempts] == ["success"], "expected one successful attempt"'

say "done: schedr in the mesh, its API behind the tokens, the docs open, a drain recorded"
