{{/*
Expand the name of the chart.
*/}}
{{- define "fulfilbox.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "fulfilbox.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "fulfilbox.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "fulfilbox.labels" -}}
helm.sh/chart: {{ include "fulfilbox.chart" . }}
{{ include "fulfilbox.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "fulfilbox.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fulfilbox.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Database URL
*/}}
{{- define "fulfilbox.databaseUrl" -}}
postgresql://{{ .Values.secrets.postgresUser }}:{{ .Values.secrets.postgresPassword }}@{{ .Release.Name }}-postgres:5432/{{ .Values.secrets.postgresDB }}
{{- end }}

{{/*
Redis URL
*/}}
{{- define "fulfilbox.redisUrl" -}}
redis://{{ .Release.Name }}-redis:6379/0
{{- end }}

{{/*
Kafka Broker URL
*/}}
{{- define "fulfilbox.kafkaBrokerUrl" -}}
{{ .Release.Name }}-kafka:9092
{{- end }}
