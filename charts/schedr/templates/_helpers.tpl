{{/* The release's name for its objects. */}}
{{- define "schedr.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* The labels every object carries. */}}
{{- define "schedr.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* The labels selecting the pod. */}}
{{- define "schedr.selector" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: standalone
{{- end -}}

{{/* The labels selecting one role's pods (the argument: the root context and the role). */}}
{{- define "schedr.roleSelector" -}}
{{- $ctx := index . 0 -}}
app.kubernetes.io/name: {{ $ctx.Chart.Name }}
app.kubernetes.io/instance: {{ $ctx.Release.Name }}
app.kubernetes.io/component: {{ index . 1 }}
{{- end -}}

{{/* standalone or distributed; distributed needs Postgres. */}}
{{- define "schedr.checkMode" -}}
{{- if not (has .Values.mode (list "standalone" "distributed")) -}}
{{- fail (printf "mode %q isn't one of \"standalone\", \"distributed\"" .Values.mode) -}}
{{- end -}}
{{- if and (eq .Values.mode "distributed") (not (or .Values.database.postgres.host .Values.database.existingSecret)) -}}
{{- fail "mode distributed needs Postgres: set database.postgres.host (or database.existingSecret); SQLite is for one pod" -}}
{{- end -}}
{{- end -}}

{{/*
The database URL (the Secret's, else SQLite on the data volume) and the configuration, for
both the migration and schedr.
*/}}
{{- define "schedr.databaseEnv" -}}
{{- $pg := .Values.database.postgres }}
{{- if and .Values.database.existingSecret $pg.host }}
{{- fail "set database.existingSecret (a whole URL) or database.postgres.host, not both" }}
{{- end }}
{{- if and $pg.host .Values.database.notificationsUrlKey }}
{{- fail "database.notificationsUrlKey belongs to database.existingSecret: with database.postgres.host, set database.postgres.notificationsHost" }}
{{- end }}
{{- if $pg.host }}
{{- $query := $pg.params | default "" | trimPrefix "?" }}
{{- $query = ternary (printf "?%s" $query) "" (ne $query "") }}
{{- $secret := required "database.postgres.existingSecret is required with database.postgres.host (its user and password)" $pg.existingSecret }}
# The user and password for the URLs below ($(...) is filled in by Kubernetes).
- name: SCHEDR_DB_USER
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ $pg.userKey }}
- name: SCHEDR_DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ $pg.passwordKey }}
- name: SCHEDR_CONFIG__DATABASE__URL
  value: {{ printf "postgres://$(SCHEDR_DB_USER):$(SCHEDR_DB_PASSWORD)@%s:%v/%s%s" $pg.host $pg.port $pg.name $query | quote }}
{{- with $pg.notificationsHost }}
- name: SCHEDR_CONFIG__DATABASE__NOTIFICATIONS_URL
  value: {{ printf "postgres://$(SCHEDR_DB_USER):$(SCHEDR_DB_PASSWORD)@%s:%v/%s%s" . $pg.notificationsPort $pg.name $query | quote }}
{{- end }}
{{- else if .Values.database.existingSecret }}
- name: SCHEDR_CONFIG__DATABASE__URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.database.existingSecret }}
      key: {{ .Values.database.secretKey }}
{{- with .Values.database.notificationsUrlKey }}
- name: SCHEDR_CONFIG__DATABASE__NOTIFICATIONS_URL
  valueFrom:
    secretKeyRef:
      name: {{ $.Values.database.existingSecret }}
      key: {{ . }}
{{- end }}
{{- else }}
- name: SCHEDR_CONFIG__DATABASE__URL
  value: sqlite:///var/lib/schedr/schedr.db
{{- end }}
- name: SCHEDR_CONFIG__DATABASE__NOTIFICATIONS
  value: {{ .Values.database.notifications | quote }}
{{ include "schedr.extraEnv" . }}
{{- end }}

{{/* The configuration of values: config, passEnv, env (every role). */}}
{{- define "schedr.extraEnv" -}}
{{- if and .Values.passEnv (hasKey .Values.config "WORKER__PASS_ENV") }}
{{- fail "set passEnv or config.WORKER__PASS_ENV, not both" }}
{{- end }}
{{- with .Values.passEnv }}
# The variables tasks get from schedr's environment (tasks get a clean one otherwise).
- name: SCHEDR_CONFIG__WORKER__PASS_ENV
  value: {{ toJson . | quote }}
{{- end }}
{{- range $key, $value := .Values.config }}
- name: SCHEDR_CONFIG__{{ $key }}
  value: {{ $value | quote }}
{{- end }}
{{- with .Values.env }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/* The ServiceAccount the pods run as: the chart's, or an existing one. */}}
{{- define "schedr.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- .Values.serviceAccount.name | default (include "schedr.fullname" .) -}}
{{- else -}}
{{- required "serviceAccount.name is required with serviceAccount.create false (an existing ServiceAccount)" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
The ServiceAccount of one role (the argument: the root context and the role) in mode
distributed: the role's own `serviceAccountName`; else, created by the chart, the one named
`serviceAccount.name` shared by every role (as in standalone, so moving keeps the account
anything is bound to), or one per role without a name; else the existing one every role shares.
*/}}
{{- define "schedr.roleServiceAccount" -}}
{{- $ctx := index . 0 -}}
{{- $role := index . 1 -}}
{{- $own := (index $ctx.Values $role).serviceAccountName -}}
{{- if $own -}}
{{- $own -}}
{{- else if and $ctx.Values.serviceAccount.create (not $ctx.Values.serviceAccount.name) -}}
{{- printf "%s-%s" (include "schedr.fullname" $ctx) $role -}}
{{- else -}}
{{- include "schedr.serviceAccountName" $ctx -}}
{{- end -}}
{{- end -}}

{{/* The account the Vault Secrets Operator authenticates as (vault.auth.serviceAccount). */}}
{{- define "schedr.vaultServiceAccount" -}}
{{- if .Values.vault.auth.serviceAccount -}}
{{- .Values.vault.auth.serviceAccount -}}
{{- else if eq .Values.mode "distributed" -}}
{{- include "schedr.roleServiceAccount" (list . "master") -}}
{{- else -}}
{{- include "schedr.serviceAccountName" . -}}
{{- end -}}
{{- end -}}

{{/* Whether every role runs under its own account (Istio's policies can then tell them apart). */}}
{{- define "schedr.accountsPerRole" -}}
{{- $accounts := list -}}
{{- range $role := list "master" "webserver" "worker" -}}
{{- $accounts = append $accounts (include "schedr.roleServiceAccount" (list $ $role)) -}}
{{- end -}}
{{- if eq (len (uniq $accounts)) 3 }}true{{ end -}}
{{- end -}}

{{/* The pod's security context: fsGroup 0 for the volumes unless set, except on OpenShift. */}}
{{- define "schedr.podSecurityContext" -}}
{{- $podSecurity := deepCopy (.Values.podSecurityContext | default dict) }}
{{- if not (or (hasKey $podSecurity "fsGroup") (.Capabilities.APIVersions.Has "security.openshift.io/v1")) }}
{{- $_ := set $podSecurity "fsGroup" 0 }}
{{- end }}
{{- with $podSecurity }}
securityContext:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
A pod's annotations: the role's (the argument: the root context and the role's
podAnnotations), and with istio.enabled the sidecar's and the proxy's settings, merged into
proxy settings of the role's own.
*/}}
{{- define "schedr.podAnnotations" -}}
{{- $ctx := index . 0 -}}
{{- $annotations := deepCopy (index . 1 | default dict) }}
{{- if $ctx.Values.istio.enabled }}
{{- /* schedr starts once its proxy is ready (it reaches the database and the master through
it), and the proxy stays until schedr's connections close: a drain's results still reach the
master and the database (without this a classic sidecar stops 5 s after SIGTERM). */}}
{{- $_ := set $annotations "sidecar.istio.io/inject" "true" }}
{{- $proxy := fromYaml (index $annotations "proxy.istio.io/config" | default "{}") }}
{{- $_ := set $proxy "holdApplicationUntilProxyStarts" true }}
{{- $metadata := $proxy.proxyMetadata | default dict }}
{{- $_ := set $metadata "EXIT_ON_ZERO_ACTIVE_CONNECTIONS" "true" }}
{{- $_ := set $proxy "proxyMetadata" $metadata }}
{{- $_ := set $annotations "proxy.istio.io/config" (toJson $proxy) }}
{{- end }}
{{- with $annotations }}
annotations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
The names of the master's gRPC port and the workers' log port, for Istio's protocol detection:
in tls.mode app they carry schedr's own TLS, which a sidecar must pass through (`tls-`), not
parse as gRPC or HTTP.
*/}}
{{- define "schedr.grpcPortName" -}}
{{- ternary "tls-grpc" "grpc" (eq .Values.tls.mode "app") -}}
{{- end -}}
{{- define "schedr.logPortName" -}}
{{- ternary "tls-logs" "http-logs" (eq .Values.tls.mode "app") -}}
{{- end -}}

{{/* The TLS Secret of one role (the argument: the root context and the role). */}}
{{- define "schedr.tlsSecret" -}}
{{- $ctx := index . 0 -}}
{{- $role := index . 1 -}}
{{- (index $ctx.Values.tls.existingSecrets $role) | default (printf "%s-%s-tls" (include "schedr.fullname" $ctx) $role) -}}
{{- end -}}

{{/* The master's host names (its Service), which its certificate names. */}}
{{- define "schedr.masterHosts" -}}
{{- $name := printf "%s-master" (include "schedr.fullname" .) -}}
{{- $ns := .Release.Namespace -}}
{{- toJson (list $name (printf "%s.%s" $name $ns) (printf "%s.%s.svc" $name $ns) (printf "%s.%s.svc.%s" $name $ns .Values.clusterDomain)) -}}
{{- end -}}

{{/* The TLS settings of one role (the argument: the root context and the role), as env. */}}
{{- define "schedr.tlsEnv" -}}
{{- $ctx := index . 0 -}}
- name: SCHEDR_CONFIG__TLS__MODE
  value: {{ $ctx.Values.tls.mode | quote }}
{{- if eq $ctx.Values.tls.mode "app" }}
- name: SCHEDR_CONFIG__TLS__CA
  value: /etc/schedr/tls/ca.crt
- name: SCHEDR_CONFIG__TLS__CERT
  value: /etc/schedr/tls/tls.crt
- name: SCHEDR_CONFIG__TLS__KEY
  value: /etc/schedr/tls/tls.key
- name: SCHEDR_CONFIG__TLS__WORKER_NAME
  value: {{ $ctx.Values.tls.workerName | quote }}
{{- end }}
{{- end -}}

{{/* tls.mode app or mesh; in app mode, where the certificates come from. */}}
{{- define "schedr.checkTls" -}}
{{- $tls := .Values.tls -}}
{{- if not (has $tls.mode (list "app" "mesh")) -}}
{{- fail (printf "tls.mode %q isn't one of \"app\", \"mesh\"" $tls.mode) -}}
{{- end -}}
{{- if eq $tls.mode "app" -}}
{{- $given := 0 -}}
{{- range $role := list "master" "webserver" "worker" -}}
{{- if index $tls.existingSecrets $role }}{{ $given = add1 $given }}{{ end -}}
{{- end -}}
{{- if and (gt $given 0) (lt $given 3) -}}
{{- fail "tls.existingSecrets: set all three (master, webserver, worker), or none" -}}
{{- end -}}
{{- if and (eq $given 0) (not $tls.certManager.issuerRef.name) (not $tls.generate) -}}
{{- fail "tls.mode app needs certificates: tls.existingSecrets, tls.certManager.issuerRef, or tls.generate (or tls.mode mesh behind a service mesh)" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* One of initContainer, onStart. */}}
{{- define "schedr.checkMigration" -}}
{{- if not (has .Values.migration (list "initContainer" "onStart")) -}}
{{- fail (printf "migration %q isn't one of \"initContainer\", \"onStart\"" .Values.migration) -}}
{{- end -}}
{{- end -}}

{{/*
The API version of Istio's `networking` or `security` group (the argument's first item): v1
where the cluster serves it (Istio 1.22 on), else v1beta1 (older Istio, OpenShift Service Mesh
2).
*/}}
{{- define "schedr.istioApi" -}}
{{- $group := printf "%s.istio.io" (index . 0) -}}
{{- $ctx := index . 1 -}}
{{- if $ctx.Capabilities.APIVersions.Has (printf "%s/v1" $group) -}}
{{- printf "%s/v1" $group -}}
{{- else -}}
{{- printf "%s/v1beta1" $group -}}
{{- end -}}
{{- end -}}

{{/* gitSync.credentials: a known type, with its Secret. */}}
{{- define "schedr.checkGitCredentials" -}}
{{- $git := .Values.gitSync.credentials -}}
{{- if not (has $git.type (list "" "token" "ssh")) -}}
{{- fail (printf "gitSync.credentials.type %q isn't one of \"token\", \"ssh\" (or empty: a public repository)" $git.type) -}}
{{- end -}}
{{- if and $git.type (not $git.secret) -}}
{{- fail "gitSync.credentials.secret is required with gitSync.credentials.type" -}}
{{- end -}}
{{- end -}}
