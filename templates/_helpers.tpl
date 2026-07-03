{{/*
Common template helpers for elastic-api-importer.
*/}}
{{- define "elastic-api-importer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "elastic-api-importer.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "elastic-api-importer.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
app.kubernetes.io/name: {{ include "elastic-api-importer.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end -}}

{{- define "elastic-api-importer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "elastic-api-importer.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}

{{- define "elastic-api-importer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "elastic-api-importer.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "elastic-api-importer.safeName" -}}
{{- regexReplaceAll "[^a-z0-9-]+" (lower .) "-" | trimAll "-" | trunc 45 | trimSuffix "-" -}}
{{- end -}}

{{- define "elastic-api-importer.payloadConfigMapName" -}}
{{- $root := index . 0 -}}
{{- $item := index . 1 -}}
{{- printf "%s-payload-%s" (include "elastic-api-importer.fullname" $root) (include "elastic-api-importer.safeName" $item.name) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
