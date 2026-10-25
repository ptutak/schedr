{{/* Where the DAGs come from (dagsPath: the image or git-sync; dags.files), for every pod with a worker. */}}

{{/* The DAG roots' values make sense together. */}}
{{- define "schedr.checkDags" -}}
{{- include "schedr.checkGitCredentials" . -}}
{{- if and .Values.gitSync.enabled (not .Values.dagsPath) }}
{{- fail "gitSync.enabled needs dagsPath (where the repository is kept)" }}
{{- end }}
{{- if and .Values.dags.files .Values.dagsPath (eq ((.Values.dagsRoot | default (base .Values.dagsPath)) | lower) "files") }}
{{- fail "dagsPath's root would be named `files`, the name of dags.files' root: set dagsRoot to another name" }}
{{- end }}
{{- if not (or .Values.dagsPath .Values.dags.files) }}
{{- fail "no DAG root: set dagsPath (DAGs in the image, or gitSync) or dags.files (charts/schedr/dags-values.sh)" }}
{{- end }}
{{- end -}}

{{/* The DAG roots, as env for schedr's container. */}}
{{- define "schedr.dagEnv" -}}
{{- $roots := list }}
{{- with .Values.dagsPath }}{{ $roots = append $roots . }}{{ end }}
{{- if .Values.dags.files }}{{ $roots = append $roots "/dag-files/..data" }}{{ end }}
- name: SCHEDR_DAG_PATH
  value: {{ join ":" $roots | quote }}
{{- if and .Values.dagsPath .Values.dagsRoot }}
- name: SCHEDR_CONFIG__DAGS__ROOTS__{{ .Values.dagsRoot | upper | replace "-" "_" }}
  value: {{ .Values.dagsPath | quote }}
{{- end }}
{{- if .Values.dags.files }}
# The ConfigMap's files: its `..data` link, which the kubelet switches to each new
# version, as git-sync switches its link (the files beside it are links through it).
- name: SCHEDR_CONFIG__DAGS__ROOTS__FILES
  value: /dag-files/..data
{{- end }}
{{- end -}}

{{/* The DAG roots' mounts in schedr's container. */}}
{{- define "schedr.dagMounts" -}}
{{- if .Values.gitSync.enabled }}
- name: dags
  mountPath: {{ dir .Values.dagsPath }}
  readOnly: true
{{- end }}
{{- if .Values.dags.files }}
- name: dag-files
  mountPath: /dag-files
  readOnly: true
{{- end }}
{{- end -}}

{{/* git-sync's sidecar, with gitSync.enabled. */}}
{{- define "schedr.gitSync" -}}
{{- if .Values.gitSync.enabled }}
{{- $git := .Values.gitSync.credentials }}
- name: git-sync
  image: {{ .Values.gitSync.image }}
  args:
    - --repo={{ required "gitSync.repo is required with gitSync.enabled" .Values.gitSync.repo }}
    - --ref={{ .Values.gitSync.ref }}
    - --root={{ dir .Values.dagsPath }}
    - --link={{ base .Values.dagsPath }}
    - --period={{ .Values.gitSync.period }}
  {{- if or $git.secret .Values.gitSync.env }}
  env:
    {{- if eq $git.type "token" }}
    - name: GITSYNC_USERNAME
      valueFrom:
        secretKeyRef:
          name: {{ $git.secret }}
          key: {{ $git.usernameKey }}
    - name: GITSYNC_PASSWORD
      valueFrom:
        secretKeyRef:
          name: {{ $git.secret }}
          key: {{ $git.passwordKey }}
    {{- else if eq $git.type "ssh" }}
    - name: GITSYNC_SSH_KEY_FILE
      value: /etc/git-secret/ssh
    - name: GITSYNC_SSH_KNOWN_HOSTS_FILE
      value: /etc/git-secret/known_hosts
    {{- end }}
    {{- with .Values.gitSync.env }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- end }}
  {{- with .Values.securityContext }}
  securityContext:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  volumeMounts:
    - name: dags
      mountPath: {{ dir .Values.dagsPath }}
    {{- if eq $git.type "ssh" }}
    - name: git-secret
      mountPath: /etc/git-secret
      readOnly: true
    {{- end }}
{{- end }}
{{- end -}}

{{/* The DAG roots' volumes. */}}
{{- define "schedr.dagVolumes" -}}
{{- if .Values.gitSync.enabled }}
- name: dags
  emptyDir: {}
{{- if eq .Values.gitSync.credentials.type "ssh" }}
- name: git-secret
  secret:
    secretName: {{ .Values.gitSync.credentials.secret }}
    defaultMode: 0400
    items:
      - key: {{ .Values.gitSync.credentials.sshKeyKey }}
        path: ssh
      - key: {{ .Values.gitSync.credentials.knownHostsKey }}
        path: known_hosts
{{- end }}
{{- end }}
{{- if .Values.dags.files }}
- name: dag-files
  configMap:
    # Every key a file of its name: no `items`, so files come and go without a restart.
    name: {{ include "schedr.fullname" . }}-dags
{{- end }}
{{- end -}}
