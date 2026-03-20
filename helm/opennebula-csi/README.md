# opennebula-csi Helm Chart

This chart deploys the SparkAI OpenNebula CSI driver into Kubernetes.

It installs:

- the CSI controller StatefulSet
- the CSI node DaemonSet
- service accounts and RBAC
- optional metrics Services and ServiceMonitors
- optional preflight validation Job
- optional StorageClass objects

Images published for this chart:

- `ghcr.io/sparkaiur/opennebula-csi:<tag>`
- `docker.io/nudevco/opennebula-csi:<tag>`

Chart repo:

- `https://sparkaiur.github.io/storage-provider-opennebula/charts/`

## What This Chart Supports

- OpenNebula disk-backed PVC provisioning on explicitly configured datastores
- datastore selection policies: `least-used`, `ordered`, `autopilot`
- local, Ceph RBD, and SparkAI CephFS datastore backends
- `ReadWriteOnce`, `ReadOnlyMany`, and CephFS-backed `ReadWriteMany`
- CSI resize, metrics, preflight checks, snapshots, and clone workflows
- gated alpha features for detached-disk expansion, CephFS expansion, CephFS snapshot/clone, CephFS self-healing, and topology accessibility

## Prerequisites

- Kubernetes cluster with CSI sidecar compatibility for the versions used by this chart
- OpenNebula API endpoint reachable from controller and node pods
- OpenNebula credentials available either in an existing Secret or inline through Helm values
- OpenNebula datastores prepared for the backends you intend to use
- for CephFS:
  - a `FILE` datastore with the SparkAI CephFS attributes
  - Ceph monitors reachable from the pods
  - provisioner and node-stage secrets available in Kubernetes

## Install

Add the chart repo:

```bash
helm repo add sparkai-opennebula https://sparkaiur.github.io/storage-provider-opennebula/charts/
helm repo update
```

### Install with an Existing Secret

Create the auth secret:

```bash
kubectl -n kube-system create secret generic opennebula-csi-auth \
  --from-literal=credentials='oneadmin:changeme'
```

Install the chart:

```bash
helm upgrade --install opennebula-csi sparkai-opennebula/opennebula-csi \
  --namespace kube-system \
  --create-namespace \
  --set credentials.existingSecret.name=opennebula-csi-auth
```

### Install with Inline Credentials

```bash
helm upgrade --install opennebula-csi sparkai-opennebula/opennebula-csi \
  --namespace kube-system \
  --create-namespace \
  --set oneApiEndpoint=http://opennebula.example.com:2633/RPC2 \
  --set credentials.inlineAuth='oneadmin:changeme'
```

Inline auth is supported for convenience. An existing Secret is the preferred production path.

## Typical Configuration Patterns

### Single Datastore

```yaml
driver:
  defaultDatastores:
    - "111"

storageClasses:
  - name: opennebula-csi
    reclaimPolicy: Delete
    allowVolumeExpansion: true
    volumeBindingMode: Immediate
    parameters:
      datastoreIDs: "111"
      fsType: "xfs"
      cache: "none"
      driver: "raw"
```

### Multiple Datastores with Fallback

```yaml
driver:
  defaultDatastores:
    - "100"
    - "101"
    - "102"
  datastoreSelectionPolicy: least-used
```

### CephFS RWX

```yaml
driver:
  defaultDatastores:
    - "300"
  allowedDatastoreTypes:
    - local
    - ceph
    - cephfs

storageClasses:
  - name: opennebula-cephfs
    reclaimPolicy: Delete
    allowVolumeExpansion: true
    volumeBindingMode: Immediate
    parameters:
      datastoreIDs: "300"
      csi.storage.k8s.io/provisioner-secret-name: cephfs-provisioner
      csi.storage.k8s.io/provisioner-secret-namespace: kube-system
      csi.storage.k8s.io/node-stage-secret-name: cephfs-node-stage
      csi.storage.k8s.io/node-stage-secret-namespace: kube-system
```

### Alpha Feature Gates

```yaml
featureGates:
  detachedDiskExpansion: true
  cephfsExpansion: true
  cephfsSnapshots: true
  cephfsClones: true
  cephfsSelfHealing: true
  topologyAccessibility: true
```

When `topologyAccessibility=true`, label nodes with:

```text
topology.opennebula.sparkaiur.io/system-ds=<opennebula-system-datastore-id>
```

## Values Reference

`Required` meanings:

- `No`: safe default exists
- `Yes`: must be set
- `Conditional`: required only when the related feature is used

### Global Values

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `nameOverride` | Override the chart name portion used for generated resource names. | `""` | No |
| `fullnameOverride` | Override the full generated release name. | `""` | No |
| `namespaceOverride` | Override the namespace used by chart resources. | `""` | No |
| `oneApiEndpoint` | OpenNebula XML-RPC endpoint exposed to the driver as `ONE_XMLRPC`. | `"http://localhost:2633/RPC2"` | Conditional |
| `imagePullSecrets` | Image pull secrets added to controller, node, and preflight pods. | `[]` | No |
| `debugPort` | Optional debug port exposed on the driver containers. | `null` | No |

### Credentials

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `credentials.existingSecret.name` | Name of an existing Secret containing OpenNebula credentials. | `""` | Conditional |
| `credentials.existingSecret.key` | Secret key inside `credentials.existingSecret.name` used for `ONE_AUTH`. | `"credentials"` | No |
| `credentials.inlineAuth` | Inline OpenNebula credentials used to create a Secret from the chart. | `""` | Conditional |

One of `credentials.existingSecret.name` or `credentials.inlineAuth` must be set.

### Image

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `image.repository` | Driver image repository used by controller, node, and default preflight image selection. | `"ghcr.io/sparkaiur/opennebula-csi"` | No |
| `image.tag` | Driver image tag. | `"v0.3.1"` | No |
| `image.pullPolicy` | Image pull policy for the driver image. | `"IfNotPresent"` | No |

### Driver

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `driver.logLevel` | Kubernetes `-v` log level passed to the driver binary. | `5` | No |
| `driver.defaultDatastores` | Default list of OpenNebula datastore IDs or aliases used when StorageClasses do not override `datastoreIDs`. | `[]` | Conditional |
| `driver.datastoreSelectionPolicy` | Default datastore selection policy. Supported values: `least-used`, `ordered`, `autopilot`. | `"least-used"` | No |
| `driver.allowedDatastoreTypes` | Allowed backend types for provisioning. | `["local","ceph","cephfs"]` | No |
| `driver.extraArgs` | Extra CLI args appended to both controller and node driver containers. | `[]` | No |
| `driver.env` | Additional environment variables appended to both controller and node driver containers. Useful for advanced overrides such as `ONE_CSI_NODE_TOPOLOGY_SYSTEM_DS`. | `[]` | No |

At least one datastore source must be configured through `driver.defaultDatastores` or StorageClass `parameters.datastoreIDs`.

### Feature Gates

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `featureGates.compatibilityAwareSelection` | Enable compatibility-aware filtering for datastores such as `COMPATIBLE_SYS_DS`. | `true` | No |
| `featureGates.detachedDiskExpansion` | Enable detached persistent-disk expansion through image-level resize. | `false` | No |
| `featureGates.cephfsExpansion` | Enable CephFS dynamic subvolume expansion. | `false` | No |
| `featureGates.cephfsSnapshots` | Enable CephFS snapshot RPC flows. | `false` | No |
| `featureGates.cephfsClones` | Enable CephFS PVC clone and snapshot restore flows. | `false` | No |
| `featureGates.cephfsSelfHealing` | Enable stale CephFS mount lazy-unmount/remount recovery in node stage. | `false` | No |
| `featureGates.topologyAccessibility` | Enable topology capability advertisement and `accessible_topology` handling. | `false` | No |

### Controller

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `controller.replicaCount` | Number of controller replicas. | `1` | No |
| `controller.podAnnotations` | Extra annotations for the controller pod template. | `{}` | No |
| `controller.resources` | Controller pod resource requests and limits. | `{}` | No |
| `controller.nodeSelector` | Node selector for the controller StatefulSet. | `{}` | No |
| `controller.tolerations` | Tolerations for the controller StatefulSet. | `[]` | No |
| `controller.affinity` | Affinity rules for the controller StatefulSet. | `{}` | No |
| `controller.extraArgs` | Extra CLI args appended only to the controller driver container. | `[]` | No |
| `controller.extraEnv` | Extra environment variables appended only to the controller driver container. | `[]` | No |

### Node

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `node.podAnnotations` | Extra annotations for the node DaemonSet pod template. | `{}` | No |
| `node.resources` | Node pod resource requests and limits. | `{}` | No |
| `node.nodeSelector` | Node selector for the node DaemonSet. | `{}` | No |
| `node.tolerations` | Tolerations for the node DaemonSet. | `[{"operator":"Exists"}]` | No |
| `node.affinity` | Affinity rules for the node DaemonSet. | `{}` | No |
| `node.extraArgs` | Extra CLI args appended only to the node driver container. | `[]` | No |
| `node.extraEnv` | Extra environment variables appended only to the node driver container. | `[]` | No |

### Metrics

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `metrics.enabled` | Enable sidecar metrics ports and metrics Services. | `true` | No |
| `metrics.path` | HTTP path used by ServiceMonitors for driver-native metrics. | `"/metrics"` | No |
| `metrics.driver.enabled` | Expose driver-native metrics server from controller and node pods. | `true` | No |
| `metrics.driver.port` | Driver-native metrics container and Service port. | `9810` | No |
| `metrics.controller.service.annotations` | Extra annotations for the controller metrics Service. | `{}` | No |
| `metrics.controller.service.labels` | Extra labels for the controller metrics Service. | `{}` | No |
| `metrics.controller.serviceMonitor.enabled` | Create a Prometheus Operator ServiceMonitor for controller metrics. | `false` | No |
| `metrics.controller.serviceMonitor.namespace` | Namespace for the controller ServiceMonitor. Empty means release namespace. | `""` | No |
| `metrics.controller.serviceMonitor.interval` | Scrape interval for the controller ServiceMonitor. | `"30s"` | No |
| `metrics.controller.serviceMonitor.scrapeTimeout` | Scrape timeout for the controller ServiceMonitor. | `"10s"` | No |
| `metrics.controller.serviceMonitor.labels` | Extra labels for the controller ServiceMonitor. | `{}` | No |
| `metrics.controller.serviceMonitor.annotations` | Extra annotations for the controller ServiceMonitor. | `{}` | No |
| `metrics.node.service.annotations` | Extra annotations for the node metrics Service. | `{}` | No |
| `metrics.node.service.labels` | Extra labels for the node metrics Service. | `{}` | No |
| `metrics.node.serviceMonitor.enabled` | Create a Prometheus Operator ServiceMonitor for node metrics. | `false` | No |
| `metrics.node.serviceMonitor.namespace` | Namespace for the node ServiceMonitor. Empty means release namespace. | `""` | No |
| `metrics.node.serviceMonitor.interval` | Scrape interval for the node ServiceMonitor. | `"30s"` | No |
| `metrics.node.serviceMonitor.scrapeTimeout` | Scrape timeout for the node ServiceMonitor. | `"10s"` | No |
| `metrics.node.serviceMonitor.labels` | Extra labels for the node ServiceMonitor. | `{}` | No |
| `metrics.node.serviceMonitor.annotations` | Extra annotations for the node ServiceMonitor. | `{}` | No |
| `metrics.ports.provisioner` | `csi-provisioner` metrics port. | `8685` | No |
| `metrics.ports.attacher` | `csi-attacher` metrics port. | `8686` | No |
| `metrics.ports.resizer` | `csi-resizer` metrics port. | `8687` | No |
| `metrics.ports.registrar` | `csi-node-driver-registrar` metrics port. | `8688` | No |

### Preflight

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `preflight.enabled` | Create the optional preflight Job. | `false` | No |
| `preflight.failReleaseOnError` | Add Helm hook annotations so failed preflight blocks the release. | `true` | No |
| `preflight.image.repository` | Override image repository for the preflight Job. Empty falls back to `image.repository`. | `""` | No |
| `preflight.image.tag` | Override image tag for the preflight Job. Empty falls back to `image.tag`. | `""` | No |
| `preflight.datastores` | Datastore identifiers checked by preflight. | `[]` | No |
| `preflight.nodeStageSecretRefs` | `namespace/name` secret refs validated for CephFS node stage. | `[]` | Conditional |
| `preflight.provisionerSecretRefs` | `namespace/name` secret refs validated for CephFS provisioning. | `[]` | Conditional |

`preflight.nodeStageSecretRefs` and `preflight.provisionerSecretRefs` are only needed when validating CephFS secret wiring.

### Snapshotter

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `snapshotter.enabled` | Deploy the `csi-snapshotter` sidecar in the controller pod. | `true` | No |
| `snapshotter.image` | Snapshotter image reference. | `"registry.k8s.io/sig-storage/csi-snapshotter:v8.2.1"` | No |
| `snapshotter.extraArgs` | Additional args for the snapshotter sidecar. | `[]` | No |

### StorageClasses

`storageClasses` is an array. Each item renders one `StorageClass`.

| Parameter | Description | Default | Required |
| --- | --- | --- | --- |
| `storageClasses[].name` | StorageClass name. | none | Yes |
| `storageClasses[].annotations` | Extra annotations on the StorageClass. | none | No |
| `storageClasses[].labels` | Extra labels on the StorageClass. | none | No |
| `storageClasses[].reclaimPolicy` | StorageClass reclaim policy. | none | Conditional |
| `storageClasses[].allowVolumeExpansion` | Whether PVC resize is allowed for that StorageClass. | none | No |
| `storageClasses[].mountOptions` | StorageClass mount options. | none | No |
| `storageClasses[].volumeBindingMode` | StorageClass binding mode. | none | No |
| `storageClasses[].parameters` | Driver parameters injected into the StorageClass. | none | No |

Common `storageClasses[].parameters` used by this driver:

- `datastoreIDs`
- `datastoreSelectionPolicy`
- `fsType`
- `sharedFilesystemPath`
- `sharedFilesystemSubvolumeGroup`
- `csi.storage.k8s.io/provisioner-secret-name`
- `csi.storage.k8s.io/provisioner-secret-namespace`
- `csi.storage.k8s.io/node-stage-secret-name`
- `csi.storage.k8s.io/node-stage-secret-namespace`
- OpenNebula disk tuning keys such as `cache`, `driver`, `io`, `devPrefix`

### Required Values by Scenario

| Scenario | Minimum required values |
| --- | --- |
| Basic disk-backed install | `oneApiEndpoint` plus either `credentials.existingSecret.name` or `credentials.inlineAuth` |
| Default provisioning without StorageClass overrides | `driver.defaultDatastores` |
| StorageClass-managed provisioning | `storageClasses[].name` plus `storageClasses[].parameters.datastoreIDs` or `driver.defaultDatastores` |
| CephFS RWX | CephFS datastore IDs, StorageClass secret refs, Kubernetes Secrets with `adminID/adminKey` and `userID/userKey` |
| Topology accessibility alpha | `featureGates.topologyAccessibility=true` plus node labels `topology.opennebula.sparkaiur.io/system-ds=<id>` |
| Detached disk expansion alpha | `featureGates.detachedDiskExpansion=true` |
| CephFS expansion alpha | `featureGates.cephfsExpansion=true` |
| CephFS snapshots alpha | `featureGates.cephfsSnapshots=true` |
| CephFS clones alpha | `featureGates.cephfsClones=true` |

## Useful Commands

Render locally:

```bash
helm template opennebula-csi ./helm/opennebula-csi \
  --set credentials.existingSecret.name=opennebula-csi-auth
```

Lint locally:

```bash
helm lint ./helm/opennebula-csi \
  --set credentials.existingSecret.name=opennebula-csi-auth
```

Upgrade with a values file:

```bash
helm upgrade --install opennebula-csi ./helm/opennebula-csi \
  --namespace kube-system \
  --create-namespace \
  --set credentials.existingSecret.name=opennebula-csi-auth \
  --values examples/omni-values.yaml
```

## Related Examples

- `examples/helm-values-existing-secret.yaml`
- `examples/helm-values-single-datastore.yaml`
- `examples/helm-values-multi-datastore.yaml`
- `examples/helm-values-cephfs-dynamic.yaml`
- `examples/helm-values-feature-gates-alpha.yaml`
