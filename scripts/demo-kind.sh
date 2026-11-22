#!/usr/bin/env bash
# `make demo-kind` (M5.2i7): the Kubernetes extension presented on a LOCAL kind cluster. Brings
# up, each step kept between runs: the kind cluster, Headlamp in it (the Kubernetes SIG's web UI;
# its admin token printed, opened through a port-forward), and a standalone here, in the
# foreground, whose worker has the Kubernetes plugin; its demo DAGs
# (examples/extensions/kubernetes_demo.py) run a Job every 2 minutes, the pod's log in the
# attempt's, its phases and Headlamp links on the task, its metrics on the UI's Extensions page.
# Ctrl-C stops the standalone and the port-forward. Every kubectl and helm call names the kind
# cluster's context. Port 8080 must be free.
#
# Headlamp, not the Kubernetes Dashboard: that project was archived in January 2026 (the user,
# 2026-11-22). Its chart's version is pinned.
#
# Usage: scripts/demo-kind.sh [up|down]
#   up (default)        bring it up
#   down                remove Headlamp and the demo's Jobs; the cluster is kept
#   DELETE_CLUSTER=1    with down, delete the cluster too
#   CLUSTER=schedr-demo the kind cluster
#   UI_PORT=4466        Headlamp's local port
#   CONTAINER=podman    the container tool
set -euo pipefail
CLUSTER=${CLUSTER:-schedr-demo}
CONTEXT=kind-$CLUSTER
NAMESPACE=schedr-demo
UI_NAMESPACE=headlamp
UI_PORT=${UI_PORT:-4466}
HEADLAMP_REPO=https://kubernetes-sigs.github.io/headlamp/
HEADLAMP_VERSION=0.46.0
ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=kind-lib.sh
source "$ROOT/scripts/kind-lib.sh"
DIR=$ROOT/.schedr/demo-kind
PLUGIN=$ROOT/examples/extensions/kubernetes/schedr-ext-kubernetes
FORWARD_PID=$DIR/port-forward.pid
UNPAUSE_PID=
# The local standalone, whatever SCHEDR_SERVER the shell has (agy's review of M5.2i7).
export SCHEDR_SERVER=http://127.0.0.1:8080

u() { kubectl --context "$CONTEXT" -n "$UI_NAMESPACE" "$@"; }

# The port-forward this script started, if it still runs, gone before this returns; the
# background unpause too.
stop_forward() {
    local pid
    if [ -n "$UNPAUSE_PID" ]; then
        kill -s TERM "$UNPAUSE_PID" 2>/dev/null || true
        UNPAUSE_PID=
    fi
    if [ -f "$FORWARD_PID" ]; then
        pid=$(cat "$FORWARD_PID")
        kill -s TERM "$pid" 2>/dev/null || true
        for _ in $(seq 1 50); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
        rm -f "$FORWARD_PID"
    fi
}

# Whether `kind` lists the cluster; fails when `kind` itself does (agy's review of M5.2i7: a
# failing `kind` read as no cluster).
cluster_exists() {
    local clusters
    clusters=$(kind get clusters 2>&1) || fail "kind get clusters: $clusters"
    grep -qx "$CLUSTER" <<<"$clusters"
}

down() {
    stop_forward
    rm -f "$DIR/headlamp-token"
    if cluster_exists; then
        require_local_context
        if [ "${DELETE_CLUSTER:-0}" = 1 ]; then
            say "deleting kind cluster $CLUSTER"
            kind delete cluster --name "$CLUSTER"
            return
        fi
        start_kind_node
        say "removing Headlamp and the demo's Jobs (cluster $CLUSTER kept; DELETE_CLUSTER=1 deletes it)"
        helm --kube-context "$CONTEXT" -n "$UI_NAMESPACE" uninstall headlamp \
            --ignore-not-found >/dev/null
        # Waited for: a `demo-kind` right after would find them still terminating.
        kubectl --context "$CONTEXT" delete namespace "$UI_NAMESPACE" "$NAMESPACE" \
            --ignore-not-found --timeout=5m >/dev/null
        kubectl --context "$CONTEXT" delete clusterrolebinding schedr-demo-admin --ignore-not-found >/dev/null
    fi
    echo "down (the standalone's data stays in $DIR/data)"
}

ui() {
    say "Headlamp $HEADLAMP_VERSION (namespace $UI_NAMESPACE)"
    helm --kube-context "$CONTEXT" upgrade --install headlamp headlamp --repo "$HEADLAMP_REPO" \
        --version "$HEADLAMP_VERSION" -n "$UI_NAMESPACE" --create-namespace --wait --timeout 10m \
        >/dev/null
    # An admin account for the demo's login: this cluster is local and the demo's own.
    u create serviceaccount schedr-demo-admin --dry-run=client -o yaml | u apply -f - >/dev/null
    kubectl --context "$CONTEXT" create clusterrolebinding schedr-demo-admin \
        --clusterrole=cluster-admin --serviceaccount="$UI_NAMESPACE:schedr-demo-admin" \
        --dry-run=client -o yaml | kubectl --context "$CONTEXT" apply -f - >/dev/null
    # A new file each time, so its mode is this one (an older file's may be looser).
    rm -f "$DIR/headlamp-token"
    (umask 077 && u create token schedr-demo-admin --duration=24h >"$DIR/headlamp-token")
    stop_forward
    if curl -s -o /dev/null --max-time 2 "http://127.0.0.1:$UI_PORT/"; then
        fail "something already listens on $UI_PORT: stop it, or set UI_PORT"
    fi
    setsid kubectl --context "$CONTEXT" -n "$UI_NAMESPACE" port-forward \
        --address 127.0.0.1 svc/headlamp "$UI_PORT:80" >"$DIR/port-forward.log" 2>&1 </dev/null &
    local pid=$!
    echo "$pid" >"$FORWARD_PID"
    for _ in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || break
        curl -sf -o /dev/null --max-time 2 "http://127.0.0.1:$UI_PORT/" && return
        sleep 1
    done
    cat "$DIR/port-forward.log"
    fail "Headlamp didn't answer on http://127.0.0.1:$UI_PORT"
}

# The demo's DAGs unpaused, once the standalone lists them (at most 10 minutes: a first build).
unpause() {
    local dag
    for _ in $(seq 1 600); do
        if "$ROOT/target/debug/schedr" dags list 2>/dev/null | grep -q kind_demo_slow; then
            for dag in kind_demo kind_demo_slow; do
                "$ROOT/target/debug/schedr" dags unpause "$dag" >/dev/null
            done
            return
        fi
        sleep 1
    done
    echo "WARNING: the standalone didn't list the demo's DAGs in 10 minutes: unpause them" \
        "with \`schedr dags unpause kind_demo\` and \`kind_demo_slow\`" >&2
}

up() {
    if curl -s -o /dev/null --max-time 2 http://127.0.0.1:8080/; then
        echo "FAILED: something already listens on 8080 (a standalone?): stop it first"
        exit 1
    fi
    mkdir -p "$DIR/dags"
    say "kind cluster $CLUSTER (context $CONTEXT), the demo's Jobs in namespace $NAMESPACE"
    # Set before anything starts: a failure, Ctrl-C or a stop of the group takes the
    # port-forward and the unpause with it (agy's review of M5.2i7).
    trap stop_forward EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    cluster_exists || kind create cluster --name "$CLUSTER"
    require_local_context
    start_kind_node
    k get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl --context "$CONTEXT" create namespace "$NAMESPACE" >/dev/null
    ui
    cp "$ROOT/examples/extensions/kubernetes_demo.py" "$DIR/dags/"
    export SCHEDR_CONFIG__EXTENSIONS__KUBERNETES__COMMAND="[\"$PLUGIN\", \"--namespace\", \"$NAMESPACE\", \"--context\", \"$CONTEXT\", \"--headlamp\", \"http://127.0.0.1:$UI_PORT\"]"
    export SCHEDR_CONFIG__EXTENSIONS__KUBERNETES__VERSIONS='["1.0"]'
    cat <<EOF

== up
   Headlamp: http://127.0.0.1:$UI_PORT, the namespace $NAMESPACE's Jobs and pods
     its login token (24 hours), in $DIR/headlamp-token:
$(sed 's/^/     /' "$DIR/headlamp-token")
   schedr's UI: http://127.0.0.1:8080, its Extensions page: http://127.0.0.1:8080/extensions
     DAGs kind_demo and kind_demo_slow run every 2 minutes; the charts fill in a few minutes.
   Ctrl-C stops the standalone and the port-forward; \`make demo-kind-down\` removes the rest.

EOF
    # New DAGs start paused: the demo's are unpaused once the standalone has them.
    unpause &
    UNPAUSE_PID=$!
    make -C "$ROOT" --no-print-directory standalone DAGS="$DIR/dags" STANDALONE_DIR="$DIR/data" \
        LISTEN=127.0.0.1
}

case "${1:-up}" in
up) up ;;
down) down ;;
*)
    echo "usage: $0 [up|down]"
    exit 2
    ;;
esac
