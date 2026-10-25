{{/* Pieces of the Istio objects (istio.yaml). */}}

{{/* An AuthorizationPolicy's rule: anything from the release's namespace, with sameNamespace. */}}
{{- define "schedr.istioSameNamespaceRule" -}}
{{- if .Values.istio.authorizationPolicy.sameNamespace }}
# Anything from this namespace, to any port.
- from:
    - source:
        namespaces: [{{ .Release.Namespace | quote }}]
{{- end }}
{{- end -}}

{{/* An AuthorizationPolicy's rule: the ops port, from authorizationPolicy.ops.namespaces. */}}
{{- define "schedr.istioOpsRule" -}}
{{- with .Values.istio.authorizationPolicy.ops.namespaces }}
# The ops port, from these namespaces.
- from:
    - source:
        namespaces:
          {{- toYaml . | nindent 10 }}
  to:
    - operation:
        ports: [{{ $.Values.service.opsPort | quote }}]
{{- else }}
# The ops port: for everyone.
- to:
    - operation:
        ports: [{{ .Values.service.opsPort | quote }}]
{{- end }}
{{- end -}}
