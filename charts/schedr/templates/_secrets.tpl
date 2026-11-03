{{/*
Secret storage (secretStorage): standalone's `db` storage key, and a Vault storage read by
standalone or the workers with Vault's Kubernetes auth. Called with (list $ <standalone?>).
*/}}

{{/* The storage's name as DAGs write it, checked: environment variables lowercase it. */}}
{{- define "schedr.vaultStorageName" -}}
{{- $name := .Values.secretStorage.vault.name -}}
{{- if or (not (regexMatch "^[a-z0-9_-]+$" $name)) (contains "__" $name) (eq $name "default") -}}
{{- fail (printf "secretStorage.vault.name `%s`: lowercase letters, digits, `_` and `-`, no `__`, not `default`" $name) -}}
{{- end -}}
{{- $name -}}
{{- end }}

{{- define "schedr.secretStorageEnv" -}}
{{- $ := index . 0 -}}
{{- $standalone := index . 1 -}}
{{- $s := $.Values.secretStorage -}}
{{- if and $standalone $s.db.existingSecret }}
# The `db` storage's key (standalone only: a worker never has that storage).
- name: SCHEDR_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ $s.db.existingSecret }}
      key: {{ $s.db.secretKey }}
{{- end }}
{{- if $s.vault.enabled }}
{{- $prefix := printf "SCHEDR_CONFIG__SECRET_STORAGE__%s__" (include "schedr.vaultStorageName" $ | upper) }}
- name: {{ $prefix }}TYPE
  value: vault
- name: {{ $prefix }}ADDRESS
  value: {{ required "secretStorage.vault.address is required with secretStorage.vault.enabled" $s.vault.address | quote }}
- name: {{ $prefix }}MOUNT
  value: {{ $s.vault.mount | quote }}
{{- with $s.vault.namespace }}
- name: {{ $prefix }}NAMESPACE
  value: {{ . | quote }}
{{- end }}
{{- with $s.vault.caCert.configMap }}
- name: {{ $prefix }}CA_CERT
  value: /etc/schedr/vault-ca/{{ $s.vault.caCert.key }}
{{- end }}
- name: {{ $prefix }}AUTH__METHOD
  value: kubernetes
- name: {{ $prefix }}AUTH__ROLE
  value: {{ required "secretStorage.vault.auth.role is required with secretStorage.vault.enabled" $s.vault.auth.role | quote }}
- name: {{ $prefix }}AUTH__MOUNT
  value: {{ $s.vault.auth.mount | quote }}
- name: {{ $prefix }}AUTH__TOKEN_FILE
  value: /var/run/secrets/schedr/vault/token
{{- end }}
{{- end }}

{{- define "schedr.secretStorageMounts" -}}
{{- $s := .Values.secretStorage -}}
{{- if $s.vault.enabled }}
- name: vault-token
  mountPath: /var/run/secrets/schedr/vault
  readOnly: true
{{- if $s.vault.caCert.configMap }}
- name: vault-ca
  mountPath: /etc/schedr/vault-ca
  readOnly: true
{{- end }}
{{- end }}
{{- end }}

{{/* A service-account token for Vault's login only (the pods mount none otherwise). */}}
{{- define "schedr.secretStorageVolumes" -}}
{{- $s := .Values.secretStorage -}}
{{- if $s.vault.enabled }}
- name: vault-token
  projected:
    sources:
      - serviceAccountToken:
          path: token
          expirationSeconds: {{ $s.vault.auth.tokenExpirationSeconds }}
          {{- with $s.vault.auth.audience }}
          audience: {{ . | quote }}
          {{- end }}
{{- with $s.vault.caCert.configMap }}
- name: vault-ca
  configMap:
    name: {{ . }}
{{- end }}
{{- end }}
{{- end }}
