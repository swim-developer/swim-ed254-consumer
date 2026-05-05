{{- define "swim-ed254-consumer.labels" -}}
app: {{ .Values.appName }}
app.kubernetes.io/part-of: swim-ed254
{{- end }}

{{- define "swim-ed254-consumer.selectorLabels" -}}
app: {{ .Values.appName }}
{{- end }}
