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

{{/* Only standalone so far. */}}
{{- define "schedr.checkMode" -}}
{{- if ne .Values.mode "standalone" -}}
{{- fail (printf "mode %q isn't supported yet: only \"standalone\" (\"distributed\" comes with step 2.11)" .Values.mode) -}}
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
