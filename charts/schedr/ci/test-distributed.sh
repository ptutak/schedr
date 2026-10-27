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
[ "$(grep -c '^kind: ServiceAccount' "$WORK/out.yaml")" = 3 ] || fail "an account per role"
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
for role in master webserver worker; do
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
echo "ok: generated certificates: one CA, the right names, serverAuth and clientAuth"

# mesh: no certificates, plain URLs.
render --set tls.mode=mesh > "$WORK/out.yaml"
lacks "kind: Secret" "mesh"
lacks "SCHEDR_CONFIG__TLS__CA" "mesh"
has 'value: "http://t-schedr-master.ns.svc:8070"' "mesh's master URL"
# cert-manager: Certificates instead of Secrets.
render --set tls.certManager.issuerRef.name=ca --set tls.certManager.issuerRef.kind=ClusterIssuer \
    > "$WORK/out.yaml"
[ "$(grep -c '^kind: Certificate$' "$WORK/out.yaml")" = 3 ] || fail "a Certificate per role"
lacks "kind: Secret" "cert-manager"
has 'secretName: t-schedr-worker-tls' "cert-manager's Secrets"
# Secrets of one's own: nothing generated, theirs mounted.
render --set tls.existingSecrets.master=m --set tls.existingSecrets.webserver=w \
    --set tls.existingSecrets.worker=k > "$WORK/out.yaml"
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
echo "ok: an existing account; Istio's policies for the internal links; mTLS to the workers"

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
[ "$(grep -c 'schedr.io/certificate-names' "$WORK/out.yaml")" = 3 ] || fail "the certificates' fingerprint"
render --set tls.mode=mesh > "$WORK/out.yaml"
has "name: grpc$" "the master's port in mesh mode"
has "name: http-logs$" "the workers' port in mesh mode"
render --set serviceAccount.name=schedr-sa --set vault.enabled=true --set vault.auth.role=r \
    --set clusterDomain=corp.local > "$WORK/out.yaml"
[ "$(grep -c '^kind: ServiceAccount' "$WORK/out.yaml")" = 1 ] || fail "one shared account"
[ "$(grep -c 'serviceAccountName: schedr-sa' "$WORK/out.yaml")" = 3 ] || fail "every role as it"
has "serviceAccount: schedr-sa" "Vault's account"
has '.svc.corp.local"' "the cluster's domain"
render --set vault.enabled=true --set vault.auth.role=r > "$WORK/out.yaml"
has "serviceAccount: t-schedr-master" "Vault's account with an account per role"
echo "ok: port names by TLS mode, a shared account, Vault's account, fingerprints, the domain"

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
