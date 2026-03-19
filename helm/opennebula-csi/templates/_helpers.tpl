{{/*
Expand the name of the chart.
*/}}
{{- define "opennebula-csi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "opennebula-csi.fullname" -}}
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
{{- define "opennebula-csi.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "opennebula-csi.namespace" -}}
{{- default .Release.Namespace .Values.namespaceOverride -}}
{{- end }}

{{- define "opennebula-csi.controllerServiceAccountName" -}}
{{- printf "%s-controller-sa" (include "opennebula-csi.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "opennebula-csi.nodeServiceAccountName" -}}
{{- printf "%s-node-sa" (include "opennebula-csi.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "opennebula-csi.authSecretName" -}}
{{- if .Values.credentials.existingSecret.name -}}
{{- .Values.credentials.existingSecret.name -}}
{{- else -}}
{{- $_ := required "Set credentials.existingSecret.name or credentials.inlineAuth" .Values.credentials.inlineAuth -}}
{{- printf "%s-auth" (include "opennebula-csi.fullname" .) -}}
{{- end -}}
{{- end }}

{{- define "opennebula-csi.authSecretKey" -}}
{{- default "credentials" .Values.credentials.existingSecret.key -}}
{{- end }}

{{- define "opennebula-csi.driverCommonEnv" -}}
- name: ONE_XMLRPC
  value: {{ .Values.oneApiEndpoint | quote }}
- name: ONE_AUTH
  valueFrom:
    secretKeyRef:
      name: {{ include "opennebula-csi.authSecretName" . }}
      key: {{ include "opennebula-csi.authSecretKey" . }}
{{- if .Values.driver.defaultDatastores }}
- name: ONE_CSI_DEFAULT_DATASTORES
  value: {{ join "," .Values.driver.defaultDatastores | quote }}
{{- end }}
{{- if .Values.driver.datastoreSelectionPolicy }}
- name: ONE_CSI_DATASTORE_SELECTION_POLICY
  value: {{ .Values.driver.datastoreSelectionPolicy | quote }}
{{- end }}
{{- if .Values.driver.allowedDatastoreTypes }}
- name: ONE_CSI_ALLOWED_DATASTORE_TYPES
  value: {{ join "," .Values.driver.allowedDatastoreTypes | quote }}
{{- end }}
{{- with .Values.driver.env }}
{{ toYaml . }}
{{- end }}
{{- end }}
