{{/*
Expand the name of the chart.
*/}}
{{- define "event-exporter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "event-exporter.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- include "event-exporter.name" . | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "event-exporter.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "event-exporter.labels" -}}
helm.sh/chart: {{ include "event-exporter.chart" . }}
{{ include "event-exporter.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "event-exporter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "event-exporter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}


{{/*
Whether event-exporter needs Pod watch RBAC for realtime enrichment.
*/}}
{{- define "event-exporter.podWatchEnabled" -}}
{{- if and .Values.exporter.enrichment.enabled .Values.exporter.enrichment.podMetadata.enabled (eq .Values.exporter.enrichment.podMetadata.realtimeSource "kubernetes-watch-cache") -}}true{{- else -}}false{{- end -}}
{{- end }}
