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
{{- if .Values.database.existingSecret }}
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
