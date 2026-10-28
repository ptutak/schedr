#!/usr/bin/env bash
# Tests the chart's mode distributed (make chart-check): what each combination of values
# renders, the generated certificates checked with openssl (chain, names, key usages), and the
# combinations refused.
set -euo pipefail
CHART=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
fail() { echo "test-distributed.sh: $*" >&2; exit 1; }
PG=(--set mode=distributed --set database.postgres.host=pg --set database.postgres.existingSecret=pg)
render() { helm template t "$CHART" --namespace ns "${PG[@]}" "$@"; }
has() { grep -q -- "$1" "$WORK/out.yaml" || fail "$2: no \`$1\`"; }
lacks() { ! grep -q -- "$1" "$WORK/out.yaml" || fail "$2: \`$1\` there"; }
refused() { if helm template t "$CHART" "$@" >/dev/null 2>&1; then fail "not refused: $*"; fi; }

# The roles, their Services, the budget, an account each; mTLS with generated certificates.
render > "$WORK/out.yaml"
for object in "kind: Deployment" "name: t-schedr-master" "name: t-schedr-webserver" \
    "kind: StatefulSet" "name: t-schedr-worker" "kind: PodDisruptionBudget" \
    "clusterIP: None" "strategy:" "type: Recreate"; do
    has "$object" "the roles"
done
[ "$(grep -c '^kind: ServiceAccount' "$WORK/out.yaml")" = 4 ] || fail "an account per role (the UI's too)"
has 'value: "https://t-schedr-master.ns.svc:8070"' "the workers' master URL"
# shellcheck disable=SC2016 # Kubernetes' $(VAR), not the shell's.
has 'value: "$(POD_NAME).t-schedr-worker.ns.svc.cluster.local"' "the workers' log address"
has 'args: \["db", "migrate", "--wait"\]' "the master's migration"
[ "$(grep -c '^kind: StatefulSet' "$WORK/out.yaml")" = 1 ] || fail "only the workers' StatefulSet"
for template in worker master webserver; do
    count=$(helm template t "$CHART" "${PG[@]}" --show-only "templates/$template.yaml" \
        | grep -c 'SCHEDR_CONFIG__INTERPRETERS__PYTHON__FORK_SERVER' || true)
    want=$([ "$template" = worker ] && echo 1 || echo 0)
    [ "$count" = "$want" ] || fail "the fork server on the workers only ($template: $count)"
done
echo "ok: the roles, their Services, the budget, an account each"

# The generated certificates: one CA, the names each role is checked by, both usages.
secret() {
    sed -n "/^  name: t-schedr-$1-tls\$/,/^---/p" "$WORK/out.yaml" \
        | sed -n "s/^  $2: \\(.*\\)\$/\\1/p" | base64 -d
}
for role in master webserver worker ui; do
    secret "$role" ca.crt > "$WORK/$role-ca.pem"
    secret "$role" tls.crt > "$WORK/$role.pem"
    secret "$role" tls.key > "$WORK/$role.key"
    openssl verify -CAfile "$WORK/master-ca.pem" "$WORK/$role.pem" >/dev/null \
        || fail "$role: not signed by the CA"
    cmp -s "$WORK/master-ca.pem" "$WORK/$role-ca.pem" || fail "$role: another CA"
    openssl x509 -in "$WORK/$role.pem" -noout -text > "$WORK/$role.txt"
    grep -q 'TLS Web Server Authentication, TLS Web Client Authentication' "$WORK/$role.txt" \
        || fail "$role: not both serverAuth and clientAuth"
    [ "$(openssl x509 -in "$WORK/$role.pem" -noout -modulus)" \
        = "$(openssl rsa -in "$WORK/$role.key" -noout -modulus 2>/dev/null)" ] \
        || fail "$role: the key isn't the certificate's"
done
grep -q 'DNS:t-schedr-master.ns.svc,' "$WORK/master.txt" || fail "the master's names"
grep -q 'DNS:schedr-worker$' "$WORK/worker.txt" || fail "the workers' name"
grep -q 'DNS:schedr-ui$' "$WORK/ui.txt" || fail "the UI's name"
grep -q 'DNS:t-schedr.ns.svc.cluster.local' "$WORK/webserver.txt" \
    || fail "the webserver's certificate naming the Service the UI reaches it by"
echo "ok: generated certificates: one CA, the right names, serverAuth and clientAuth"

# mesh: no certificates, plain URLs.
render --set tls.mode=mesh > "$WORK/out.yaml"
lacks "type: kubernetes.io/tls" "mesh"
lacks "SCHEDR_CONFIG__TLS__CA" "mesh"
has 'value: "http://t-schedr-master.ns.svc:8070"' "mesh's master URL"
# cert-manager: Certificates instead of Secrets.
render --set tls.certManager.issuerRef.name=ca --set tls.certManager.issuerRef.kind=ClusterIssuer \
    > "$WORK/out.yaml"
[ "$(grep -c '^kind: Certificate$' "$WORK/out.yaml")" = 4 ] || fail "a Certificate per role"
lacks "kind: Secret" "cert-manager"
has 'secretName: t-schedr-worker-tls' "cert-manager's Secrets"
# Secrets of one's own: nothing generated, theirs mounted.
render --set tls.existingSecrets.master=m --set tls.existingSecrets.webserver=w \
    --set tls.existingSecrets.worker=k --set tls.existingSecrets.ui=u > "$WORK/out.yaml"
lacks "kind: Secret" "existing Secrets"
has "secretName: k" "the workers' own Secret"
echo "ok: mesh, cert-manager, Secrets of one's own"

# An existing account shared by every role; Istio's internal policies then by namespace.
render -f "$CHART/ci/istio-values.yaml" > "$WORK/out.yaml"
lacks "kind: ServiceAccount" "an existing account"
has "serviceAccountName: mesh-sa" "an existing account"
lacks "principals: \[\"\*/ns/" "a shared account"
render -f "$CHART/ci/istio-values.yaml" --set serviceAccount.create=true \
    --set serviceAccount.name= > "$WORK/out.yaml"
has 'principals: \["\*/ns/ns/sa/t-schedr-worker"\]' "the master's policy"
has 'principals: \["\*/ns/ns/sa/t-schedr-webserver"\]' "the workers' policy"
has 'ports: \["8070"\]' "the master's gRPC port in its policy"
has 'ports: \["8081"\]' "the log endpoint in the workers' policy"
has "kind: DestinationRule" "Istio's mTLS to the workers' headless Service"
has "mode: ISTIO_MUTUAL" "Istio's mTLS to the workers' headless Service"
render -f "$CHART/ci/istio-values.yaml" --set serviceAccount.create=true \
    --set serviceAccount.name= --api-versions security.istio.io/v1 \
    --api-versions networking.istio.io/v1 > "$WORK/out.yaml"
has 'prefix: "/service/schedr/api/"' "the API under /api/ beside the UI"
has 'host: t-schedr-ui.ns.svc.cluster.local' "the UI at the root"
has 'name: t-schedr-ui$' "the UI's policy"
has 'principals: \["\*/ns/ns/sa/t-schedr-ui"\]' "the webserver's policy letting the UI in"
has 'ports: \["8080", "8083"\]' "the UI's calls: the API and the UI port"
grep -A3 '^kind: RequestAuthentication' "$WORK/out.yaml" | grep -q 'name: t-schedr-ui$' \
    || fail "tokens checked at the UI's pods too (its policy's requestPrincipals)"
grep -A4 -- '- tools$' "$WORK/out.yaml" | grep -q 'ports: \["8080", "8083"\]' \
    || fail "api.namespaces also on the UI port (a schedr ui elsewhere, its certificate)"
render -f "$CHART/ci/istio-values.yaml" --set clusterDomain=corp.local > "$WORK/out.yaml"
has 'host: t-schedr.ns.svc.corp.local' "the gateway's route in the cluster's domain"
lacks 'svc.cluster.local' "no cluster.local with another domain"
helm template t "$CHART" --namespace ns --set mode=ui -f "$CHART/ci/istio-values.yaml" \
    --set 'ui.deployments[0].name=etl' --set 'ui.deployments[0].url=http://etl:8080' > "$WORK/out.yaml"
has 'host: t-schedr-ui.ns.svc.cluster.local' "mode ui: the gateway's route to the UI"
has 'number: 8082' "mode ui: the UI's port"
lacks 'host: t-schedr.ns' "mode ui: no API Service to route to"
lacks 'component: standalone' "mode ui: Istio's objects select the UI's pods"
for kind in RequestAuthentication AuthorizationPolicy PeerAuthentication; do
    grep -A16 "^kind: $kind" "$WORK/out.yaml" | grep -q 'component: ui' || fail "mode ui: $kind on the UI's pods"
done
# 40A: mesh mode with Istio's policy and an account each: the UI port trusts the mesh, no token.
render -f "$CHART/ci/istio-values.yaml" --set serviceAccount.create=true --set serviceAccount.name= \
    --set tls.mode=mesh --set 'istio.authorizationPolicy.ui.principals[0]=cluster.local/ns/o/sa/ui' \
    > "$WORK/out.yaml"
has 'name: SCHEDR_CONFIG__WEBSERVER__UI_PORT_IN_MESH' "mesh: the UI port trusting the mesh"
has 'name: http-ui$' "mesh: the UI port in plain HTTP (Istio's protocol detection)"
has 'url = "http://t-schedr.ns.svc.cluster.local:8083"' "mesh: the UI calls the UI port"
lacks 'name: t-schedr-ui-token' "mesh with Istio's policy: no token Secret"
lacks 'token_file' "mesh with Istio's policy: no token"
grep -A22 'name: t-schedr-webserver-ui-port$' "$WORK/out.yaml" > "$WORK/deny.yaml" \
    || fail "mesh: a DENY policy on the UI port"
grep -q 'action: DENY' "$WORK/deny.yaml" || fail "mesh: the UI port's policy denies"
grep -q '"\*/ns/ns/sa/t-schedr-ui"' "$WORK/deny.yaml" || fail "mesh: the UI's account let through"
grep -q '"cluster.local/ns/o/sa/ui"' "$WORK/deny.yaml" || fail "mesh: another release's UI let through"
grep -q 'ports: \["8083"\]' "$WORK/deny.yaml" || fail "mesh: the DENY on the UI port only"
# A shared account can't tell the UI apart: the token, as without Istio.
render -f "$CHART/ci/istio-values.yaml" --set tls.mode=mesh > "$WORK/out.yaml"
lacks 'UI_PORT_IN_MESH' "mesh with a shared account: no trusted UI port"
has 'name: t-schedr-ui-token' "mesh with a shared account: the token"
notes() { helm install --dry-run t "$CHART" --namespace ns "${PG[@]}" -f "$CHART/ci/istio-values.yaml" \
    --set tls.mode=mesh "$@" 2>&1 | sed -n '/^NOTES:/,$p'; }
notes | grep -q "with a token the chart made, not by the mesh" \
    || fail "mesh with a shared account: the notes say the token is used"
! notes --set serviceAccount.create=true --set serviceAccount.name= | grep -q "not by the mesh" \
    || fail "mesh with an account each: no note"
echo "ok: an existing account; Istio's policies for the internal links; mTLS to the workers"

# 41A: the UI signs people in as the API does, or the API is none; ui.auth empty: the webserver's.
render --set webserver.auth.mode=proxy_header --set webserver.auth.allowRemote=true > "$WORK/out.yaml"
[ "$(grep -A1 'AUTH__MODE$' "$WORK/out.yaml" | grep -c '"proxy_header"')" = 2 ] \
    || fail "ui.auth empty: the webserver's sign-in for the UI too"
render --set ui.auth.mode=basic --set ui.auth.usersSecret.name=u > "$WORK/out.yaml"
has 'secretName: u$' "the webserver's none with the UI's basic: allowed (an internal API)"
refused "${PG[@]}" --set webserver.auth.mode=basic --set webserver.auth.usersSecret.name=u \
    --set ui.auth.mode=none
refused "${PG[@]}" --set webserver.auth.mode=proxy_header --set webserver.auth.allowRemote=true \
    --set ui.auth.mode=basic --set ui.auth.usersSecret.name=u
echo "ok: the UI port trusting the mesh (40A); the UI's sign-in as the API's (41A)"

# DAGs and the tasks' environment reach the workers only.
render --set gitSync.enabled=true --set gitSync.repo=https://h/r.git \
    --set dags.files.a\\.py=eA== --set 'passEnv={MONGO_URI,API_TOKEN}' > "$WORK/out.yaml"
sed -n '/^kind: StatefulSet/,/^---/p' "$WORK/out.yaml" > "$WORK/worker.yaml"
grep -q 'name: git-sync' "$WORK/worker.yaml" || fail "git-sync in the workers"
grep -q 'name: dag-files' "$WORK/worker.yaml" || fail "the DAG files in the workers"
grep -q 'value: "\[\\"MONGO_URI\\",\\"API_TOKEN\\"\]"' "$WORK/worker.yaml" || fail "passEnv"
sed -n '/^kind: Deployment/,/^---/p' "$WORK/out.yaml" | grep -q 'git-sync\|dag-files' \
    && fail "DAGs in the master or the webserver"
echo "ok: DAGs and passEnv in the workers only"

# Review fixes: Istio's port names by TLS mode; a named created account shared; Vault's
# account; the certificates' names fingerprinted; the cluster's domain.
render > "$WORK/out.yaml"
has "name: tls-grpc" "the master's port in app mode"
has "name: tls-logs" "the workers' port in app mode"
[ "$(grep -c 'schedr.io/certificate-names' "$WORK/out.yaml")" = 4 ] || fail "the certificates' fingerprint"
render --set tls.mode=mesh > "$WORK/out.yaml"
has "name: grpc$" "the master's port in mesh mode"
has "name: http-logs$" "the workers' port in mesh mode"
render --set serviceAccount.name=schedr-sa --set vault.enabled=true --set vault.auth.role=r \
    --set clusterDomain=corp.local > "$WORK/out.yaml"
[ "$(grep -c '^kind: ServiceAccount' "$WORK/out.yaml")" = 1 ] || fail "one shared account"
[ "$(grep -c 'serviceAccountName: schedr-sa' "$WORK/out.yaml")" = 4 ] || fail "every role as it"
has "serviceAccount: schedr-sa" "Vault's account"
has '.svc.corp.local"' "the cluster's domain"
render --set vault.enabled=true --set vault.auth.role=r > "$WORK/out.yaml"
has "serviceAccount: t-schedr-master" "Vault's account with an account per role"
echo "ok: port names by TLS mode, a shared account, Vault's account, fingerprints, the domain"

# The web UI (decision 37A, M3.6): deployed with distributed, over the release's webserver.
render > "$WORK/out.yaml"
has "name: t-schedr-ui" "the UI's Deployment and Service"
has 'url = "https://t-schedr.ns.svc.cluster.local:8083"' "app mode: the webserver's UI port"
has "SCHEDR_CONFIG__WEBSERVER__UI_PORT" "the webserver's UI port"
has "name: tls-ui" "the UI port's name (TLS: a sidecar passes it through)"
has "SCHEDR_CONFIG__TLS__UI_NAME" "the name the webserver checks"
has 'args: \["--config", "/etc/schedr/ui/config.toml", "ui"\]' "the UI's command"
render --set tls.mode=mesh > "$WORK/out.yaml"
has 'url = "http://t-schedr.ns.svc.cluster.local:8080"' "mesh: the API port"
has 'token_file = "/etc/schedr/ui-tokens/0/token"' "mesh: the UI's token"
has "SCHEDR_CONFIG__WEBSERVER__UI_TOKEN_FILE" "mesh: the webserver's token"
has "name: t-schedr-ui-token" "mesh: the token made by the chart"
lacks "SCHEDR_CONFIG__WEBSERVER__UI_PORT" "mesh: no UI port"
render --set ui.enabled=false > "$WORK/out.yaml"
lacks "name: t-schedr-ui" "ui.enabled false"
has "name: tls-ui" "ui.enabled false: the UI port kept, for a schedr ui elsewhere"
has "SCHEDR_CONFIG__WEBSERVER__UI_PORT" "ui.enabled false: the UI port kept"
[ "$(grep -c '^kind: ServiceAccount' "$WORK/out.yaml")" = 3 ] || fail "no UI account"
refused "${PG[@]}" --set 'ui.deployments[0].name=default' --set 'ui.deployments[0].url=http://x:8080'
echo "ok: the UI with distributed: mTLS on the UI port, the token in mesh, off when disabled"

# mode ui: the UI alone over deployments listed; standalone without its UI, trusting a token.
ui() { helm template t "$CHART" --namespace ns --set mode=ui "$@"; }
ui --set 'ui.deployments[0].name=etl' --set 'ui.deployments[0].url=http://etl:8080' \
    --set 'ui.deployments[0].tokenSecret.name=tokens' --set 'ui.deployments[0].tokenSecret.key=etl' \
    > "$WORK/out.yaml"
has 'name = "etl"' "mode ui's deployment"
has "secretName: tokens" "mode ui's token Secret"
lacks "kind: StatefulSet" "mode ui: no standalone"
[ "$(grep -c '^kind: Service$' "$WORK/out.yaml")" = 1 ] || fail "mode ui: the UI's Service only"
[ "$(grep -c '^kind: ' "$WORK/out.yaml")" = 4 ] || fail "mode ui: an account, a ConfigMap, a Deployment, a Service"
helm template t "$CHART" --set standalone.ui=false --set webserver.uiTokenSecret.name=tok \
    > "$WORK/out.yaml"
has '"--no-ui"' "standalone without its UI"
has "SCHEDR_CONFIG__WEBSERVER__UI_TOKEN_FILE" "standalone trusting the UI's token"
echo "ok: mode ui; standalone without its UI, with a token"
refused --set mode=ui
refused --set mode=ui --set 'ui.deployments[0].name=a' --set 'ui.deployments[0].url=https://a:8083'
refused --set mode=ui --set tls.mode=mesh --set 'ui.deployments[0].name=a' \
    --set 'ui.deployments[0].url=https://a:8083'
refused "${PG[@]}" --set tls.existingSecrets.master=m --set tls.existingSecrets.webserver=w \
    --set tls.existingSecrets.worker=k
refused --set webserver.auth.mode=basic
refused --set webserver.auth.mode=ldap
echo "ok: refused: mode ui without deployments, mTLS without the UI's certificate or in mesh,"
echo "    existing Secrets without the UI's, basic without users, an unknown sign-in mode"

refused --set mode=distributed
refused "${PG[@]}" --set tls.existingSecrets.master=m
refused "${PG[@]}" --set tls.generate=false
refused "${PG[@]}" --set tls.mode=plain
refused --set 'passEnv={A}' --set config.WORKER__PASS_ENV=x
refused --set config.INTERPRETERS__PYTHON=/opt/venv/bin/python
refused --set config.INTERPRETERS__PYTHON__PATH=/opt/venv/bin/python
refused --set config.DAGS__INTERPRETER=etl --set config.INTERPRETERS__ETL=/opt/etl/python
echo "ok: refused: SQLite, part of the Secrets, no certificates, an unknown mode, passEnv twice,"
echo "    the DAG interpreter configured twice"

# The fork server's variables: for the DAG interpreter by its name; off when asked, even as
# the string "false"; with the interpreter configured, only when the fork server is off.
render --set config.DAGS__INTERPRETER=etl > "$WORK/out.yaml"
has "SCHEDR_CONFIG__INTERPRETERS__ETL__FORK_SERVER" "the fork server for a renamed DAG interpreter"
lacks "SCHEDR_CONFIG__INTERPRETERS__PYTHON__" "no other interpreter's fork server"
for off in --set --set-string; do
    render "$off" worker.forkServer.enabled=false > "$WORK/out.yaml"
    lacks "SCHEDR_CONFIG__INTERPRETERS__" "the fork server off ($off)"
done
render --set worker.forkServer.enabled=false --set config.INTERPRETERS__PYTHON=/opt/venv/bin/python \
    > "$WORK/out.yaml"
has "SCHEDR_CONFIG__INTERPRETERS__PYTHON" "an interpreter of one's own with the fork server off"
echo "ok: the fork server's variables: by the DAG interpreter's name, off, off as a string"
