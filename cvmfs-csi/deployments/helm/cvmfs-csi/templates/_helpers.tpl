{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "cvmfs-csi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "cvmfs-csi.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "cvmfs-csi.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create fully qualified app name for the node plugin.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "cvmfs-csi.nodeplugin.fullname" -}}
{{- if .Values.nodeplugin.fullnameOverride -}}
{{- .Values.nodeplugin.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.nodeplugin.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.nodeplugin.name  | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create fully qualified app name for the controller plugin.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "cvmfs-csi.controllerplugin.fullname" -}}
{{- if .Values.controllerplugin.fullnameOverride -}}
{{- .Values.controllerplugin.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.controllerplugin.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.controllerplugin.name  | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create the name of the service account to use for the node plugin.
*/}}
{{- define "cvmfs-csi.serviceAccountName.nodeplugin" -}}
{{- .Values.nodeplugin.serviceAccount.serviceAccountName | default (include "cvmfs-csi.nodeplugin.fullname" .) -}}
{{- end -}}

{{/*
Create the name of the service account to use for the node plugin.
*/}}
{{- define "cvmfs-csi.serviceAccountName.controllerplugin" -}}
{{- .Values.controllerplugin.serviceAccount.serviceAccountName | default (include "cvmfs-csi.controllerplugin.fullname" .) -}}
{{- end -}}

{{/*
Create unified labels for manila-csi components
*/}}

{{- define "cvmfs-csi.common.matchLabels" -}}
app: {{ template "cvmfs-csi.name" . }}
release: {{ .Release.Name }}
{{- end -}}

{{- define "cvmfs-csi.common.metaLabels" -}}
chart: {{ template "cvmfs-csi.chart" . }}
heritage: {{ .Release.Service }}
app: {{ template "cvmfs-csi.name" . }}
{{- if .Values.extraMetaLabels }}
{{ toYaml .Values.extraMetaLabels }}
{{- end }}
{{- end -}}

{{- define "cvmfs-csi.common.labels" -}}
{{ include "cvmfs-csi.common.metaLabels" . }}
{{ include "cvmfs-csi.common.matchLabels" . }}
{{- end -}}

{{- define "cvmfs-csi.nodeplugin.matchLabels" -}}
{{- if .Values.nodeplugin.matchLabelsOverride -}}
{{ toYaml .Values.nodeplugin.matchLabelsOverride }}
{{- else -}}
component: nodeplugin
{{ include "cvmfs-csi.common.matchLabels" . }}
{{- end -}}
{{- end -}}

{{- define "cvmfs-csi.controllerplugin.matchLabels" -}}
{{- if .Values.controllerplugin.matchLabelsOverride -}}
{{ toYaml .Values.controllerplugin.matchLabelsOverride }}
{{- else -}}
component: controllerplugin
{{ include "cvmfs-csi.common.matchLabels" . }}
{{- end -}}
{{- end -}}

{{- define "cvmfs-csi.nodeplugin.labels" -}}
{{- if .Values.nodeplugin.labelsOverride -}}
{{ toYaml .Values.nodeplugin.labelsOverride }}
{{- else -}}
{{ include "cvmfs-csi.common.metaLabels" . }}
component: nodeplugin
release: {{ .Release.Name }}
{{- end -}}
{{- end -}}

{{- define "cvmfs-csi.controllerplugin.labels" -}}
{{- if .Values.controllerplugin.labelsOverride -}}
{{ toYaml .Values.controllerplugin.labelsOverride }}
{{- else -}}
{{ include "cvmfs-csi.common.metaLabels" . }}
component: controllerplugin
release: {{ .Release.Name }}
{{- end -}}
{{- end -}}
