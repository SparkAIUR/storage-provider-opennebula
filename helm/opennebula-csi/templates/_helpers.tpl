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

{{- define "opennebula-csi.inventoryServiceAccountName" -}}
{{- printf "%s-inventory-sa" (include "opennebula-csi.fullname" .) | trunc 63 | trimSuffix "-" -}}
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

{{- define "opennebula-csi.preflightImageRepository" -}}
{{- default .Values.image.repository .Values.preflight.image.repository -}}
{{- end }}

{{- define "opennebula-csi.preflightImageTag" -}}
{{- default .Values.image.tag .Values.preflight.image.tag -}}
{{- end }}

{{- define "opennebula-csi.driverCommonEnv" -}}
{{- $root := . -}}
{{- $inventoryEnabledOverrideSet := false -}}
{{- $inventoryEnabledOverride := false -}}
{{- if kindIs "map" . -}}
  {{- if hasKey . "context" -}}
    {{- $root = get . "context" -}}
  {{- end -}}
  {{- if hasKey . "inventoryEnabledOverride" -}}
    {{- $inventoryEnabledOverrideSet = true -}}
    {{- $inventoryEnabledOverride = (get . "inventoryEnabledOverride") -}}
  {{- end -}}
{{- end -}}
{{- $inventory := (get $root.Values "inventoryController") | default dict -}}
{{- $inventoryResync := (get $inventory "resyncSeconds") | default dict -}}
{{- $inventoryValidation := (get $inventory "validation") | default dict -}}
- name: ONE_XMLRPC
  value: {{ $root.Values.oneApiEndpoint | quote }}
- name: ONE_AUTH
  valueFrom:
    secretKeyRef:
      name: {{ include "opennebula-csi.authSecretName" $root }}
      key: {{ include "opennebula-csi.authSecretKey" $root }}
{{- $inventoryEnabled := (get $inventory "enabled") | default false -}}
{{- if $inventoryEnabledOverrideSet -}}
{{- $inventoryEnabled = $inventoryEnabledOverride -}}
{{- end -}}
{{- $inventoryAuthorityMode := (get $inventory "authorityMode") | default "strict" -}}
{{- $inventoryResyncDatastores := (get $inventoryResync "datastores") | default 60 -}}
{{- $inventoryResyncNodes := (get $inventoryResync "nodes") | default 30 -}}
{{- $inventoryLeaderElectionID := (get $inventory "leaderElectionID") | default "opennebula-csi-inventory-controller" -}}
{{- $inventoryNamespace := (get $inventory "namespace") | default "" -}}
{{- if eq (trim (printf "%v" $inventoryNamespace)) "" -}}
{{- $inventoryNamespace = include "opennebula-csi.namespace" $root -}}
{{- end }}
{{- $inventoryValidationEnabled := (get $inventoryValidation "enabled") | default true -}}
{{- $inventoryValidationDefaultImage := (get $inventoryValidation "defaultImage") | default "ghcr.io/axboe/fio:latest" -}}
{{- if $root.Values.driver.defaultDatastores }}
- name: ONE_CSI_DEFAULT_DATASTORES
  value: {{ join "," $root.Values.driver.defaultDatastores | quote }}
{{- end }}
{{- if $root.Values.driver.datastoreSelectionPolicy }}
- name: ONE_CSI_DATASTORE_SELECTION_POLICY
  value: {{ $root.Values.driver.datastoreSelectionPolicy | quote }}
{{- end }}
{{- if $root.Values.driver.allowedDatastoreTypes }}
- name: ONE_CSI_ALLOWED_DATASTORE_TYPES
  value: {{ join "," $root.Values.driver.allowedDatastoreTypes | quote }}
{{- end }}
{{- if $root.Values.metrics.driver.enabled }}
- name: ONE_CSI_METRICS_ENDPOINT
  value: {{ printf ":%v" $root.Values.metrics.driver.port | quote }}
{{- end }}
- name: ONE_CSI_VM_HOTPLUG_TIMEOUT_SECONDS
  value: {{ $root.Values.driver.vmHotplugTimeoutSeconds | quote }}
- name: ONE_CSI_VM_HOTPLUG_TIMEOUT_BASE_SECONDS
  value: {{ $root.Values.driver.vmHotplugTimeoutBaseSeconds | quote }}
- name: ONE_CSI_VM_HOTPLUG_TIMEOUT_PER_100GI_SECONDS
  value: {{ $root.Values.driver.vmHotplugTimeoutPer100GiSeconds | quote }}
- name: ONE_CSI_VM_HOTPLUG_TIMEOUT_MAX_SECONDS
  value: {{ $root.Values.driver.vmHotplugTimeoutMaxSeconds | quote }}
- name: ONE_CSI_VM_HOTPLUG_STUCK_VM_COOLDOWN_SECONDS
  value: {{ $root.Values.driver.vmHotplugStuckVmCooldownSeconds | quote }}
- name: ONE_CSI_NODE_DEVICE_DISCOVERY_TIMEOUT_SECONDS
  value: {{ $root.Values.driver.nodeDeviceDiscoveryTimeoutSeconds | quote }}
- name: ONE_CSI_PREFLIGHT_LOCAL_IMMEDIATE_BINDING_POLICY
  value: {{ $root.Values.preflight.localImmediateBindingPolicy | quote }}
- name: ONE_CSI_INVENTORY_CONTROLLER_ENABLED
  value: {{ $inventoryEnabled | quote }}
- name: ONE_CSI_INVENTORY_DATASTORE_AUTHORITY_MODE
  value: {{ $inventoryAuthorityMode | quote }}
- name: ONE_CSI_INVENTORY_RESYNC_DATASTORES_SECONDS
  value: {{ $inventoryResyncDatastores | quote }}
- name: ONE_CSI_INVENTORY_RESYNC_NODES_SECONDS
  value: {{ $inventoryResyncNodes | quote }}
- name: ONE_CSI_INVENTORY_CONTROLLER_LEADER_ELECTION_ID
  value: {{ $inventoryLeaderElectionID | quote }}
- name: ONE_CSI_INVENTORY_CONTROLLER_NAMESPACE
  value: {{ $inventoryNamespace | quote }}
- name: ONE_CSI_INVENTORY_VALIDATION_ENABLED
  value: {{ $inventoryValidationEnabled | quote }}
- name: ONE_CSI_INVENTORY_VALIDATION_DEFAULT_IMAGE
  value: {{ $inventoryValidationDefaultImage | quote }}
- name: ONE_CSI_FEATURE_GATES
  value: "compatibilityAwareSelection={{ $root.Values.featureGates.compatibilityAwareSelection }},detachedDiskExpansion={{ $root.Values.featureGates.detachedDiskExpansion }},cephfsExpansion={{ $root.Values.featureGates.cephfsExpansion }},cephfsSnapshots={{ $root.Values.featureGates.cephfsSnapshots }},cephfsClones={{ $root.Values.featureGates.cephfsClones }},cephfsSelfHealing={{ $root.Values.featureGates.cephfsSelfHealing }},topologyAccessibility={{ $root.Values.featureGates.topologyAccessibility }}"
{{- with $root.Values.driver.env }}
{{ toYaml . }}
{{- end }}
{{- end }}
