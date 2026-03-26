# SparkAI OpenNebula CSI

`storage-provider-opennebula` is the SparkAI fork of the OpenNebula CSI driver for Kubernetes. It publishes container images to `ghcr.io/sparkaiur/opennebula-csi:<tag>` and `docker.io/nudevco/opennebula-csi:<tag>`, and Helm charts to `https://sparkaiur.github.io/storage-provider-opennebula/charts/`.

This fork is focused on Omni deployments on OpenNebula and removes the old requirement that a datastore literally named `default` must exist for PVC provisioning.

## Current scope

- Provision PVCs on explicitly configured OpenNebula datastores.
- Support global driver datastore defaults and per-StorageClass overrides.
- Support `least-used`, `ordered`, and `autopilot` datastore selection policies.
- Support `Filesystem` and `Block` volume modes.
- Support `ReadWriteOnce`, `ReadOnlyMany`, and CephFS-backed `ReadWriteMany` filesystem volumes.
- Support CSI volume expansion for attached volumes, detached persistent disks, and dynamic CephFS subvolumes.
- Support `NodeGetVolumeStats`, driver-native Prometheus metrics, Kubernetes Events, and PV placement annotations.
- Support preflight validation through the binary and an optional Helm Job.
- Support CSI snapshots plus PVC-to-PVC clone for disk-backed OpenNebula volumes.
- Support gated topology accessibility plus CephFS snapshot/clone and self-healing flows for staging validation.
- Support `local`, OpenNebula Ceph RBD, and SparkAI CephFS datastores.
- Keep the internal selection/provider structure ready for future `nfs` support.

## Validated v0.4.3 matrix

The `v0.4.3` release was validated on a live Omni + Talos + OpenNebula staging cluster with:

- local RWO volumes on an OpenNebula image datastore
- Ceph RBD RWO volumes on a Ceph-backed image datastore with a Ceph system datastore
- CephFS static and dynamic RWX volumes on a SparkAI CephFS `FILE` datastore
- ROX clones on the disk-backed path

Staging-validated stable features:

- Local RWO create, mount, resize, detach, reattach, and delete cleanup
- Ceph RBD RWO create, mount, resize, detach, reattach, and delete cleanup
- CephFS static RWX create, publish across nodes, and retain-on-delete behavior
- CephFS dynamic RWX create, publish across nodes, resize, snapshot create/delete, clone, snapshot restore, and delete cleanup
- Disk-path CSI snapshots and PVC clones
- `NodeGetVolumeStats`, driver metrics, PV placement annotations, and Kubernetes Events
- `detachedDiskExpansion`
- `cephfsExpansion`

Features that remain gated in `v0.4.3`:

- `cephfsSnapshots`
- `cephfsClones`
- `cephfsSelfHealing`
- `topologyAccessibility`

## Release artifacts

- Container images:
  `ghcr.io/sparkaiur/opennebula-csi:<tag>`
  `docker.io/nudevco/opennebula-csi:<tag>`
- Helm repo: `https://sparkaiur.github.io/storage-provider-opennebula/charts/`
- Chart name: `opennebula-csi`
- Source repo: `https://github.com/SparkAIUR/storage-provider-opennebula`

## Datastore selection

The driver now resolves its provisioning datastores from two layers:

1. StorageClass parameters
2. Driver defaults from environment or Helm values

If both are present, the StorageClass wins.

### Supported control parameters

These parameters are reserved by the CSI driver:

- `datastoreIDs`: CSV list of datastore identifiers, for example `100`, `100,101,102`, or `default`
- `datastoreSelectionPolicy`: `least-used`, `ordered`, or `autopilot`
- `fsType`: filesystem hint for filesystem volumes

All other StorageClass parameters are passed through to the existing disk/image handling logic. That allows tuning values such as `devPrefix`, `cache`, `driver`, and similar OpenNebula disk options.

### Driver environment variables

- `ONE_CSI_DEFAULT_DATASTORES`: global CSV datastore list, for example `100,101`
- `ONE_CSI_DATASTORE_SELECTION_POLICY`: `least-used`, `ordered`, or `autopilot`
- `ONE_CSI_ALLOWED_DATASTORE_TYPES`: CSV list, default `local,ceph,cephfs`
- `ONE_CSI_FEATURE_GATES`: CSV feature-gate map for alpha features
- `ONE_CSI_METRICS_ENDPOINT`: driver-native metrics endpoint, default `:9810`
- `ONE_CSI_VM_HOTPLUG_TIMEOUT_SECONDS`: legacy alias for the base VM hotplug timeout, default `120`
- `ONE_CSI_VM_HOTPLUG_TIMEOUT_BASE_SECONDS`: base VM hotplug timeout, default `120`
- `ONE_CSI_VM_HOTPLUG_TIMEOUT_PER_100GI_SECONDS`: additional timeout per 100 GiB bucket, default `60`
- `ONE_CSI_VM_HOTPLUG_TIMEOUT_MAX_SECONDS`: maximum VM hotplug timeout, default `900`
- `ONE_CSI_VM_HOTPLUG_STUCK_VM_COOLDOWN_SECONDS`: cooldown after a VM stays stuck in hotplug through the full timeout, default `300`

### Policy behavior

- `least-used`: sort eligible datastores by free capacity descending, then try them in that order
- `ordered`: use the configured order as-is
- `autopilot`: score eligible datastores by free-space ratio, absolute free space, recent provisioning failures, and current in-flight selections

If a candidate datastore does not have enough free capacity, the driver falls through to the next eligible datastore. If no configured datastore can satisfy the request, provisioning fails with a capacity error.
When the driver falls back to a later datastore, it emits `DatastoreFallback` events and records the chosen placement on the resulting PV annotations.

## Datastore category support

Provisioning targets must be OpenNebula datastore categories that can accept image allocation.

Current rules:

- `IMAGE`: supported for PVC provisioning
- `FILE`: supported for SparkAI CephFS RWX datastores when the datastore template exposes the required CephFS attributes
- `SYSTEM`: rejected for `CreateVolume`

For OpenNebula local-style datastores, the driver treats `local`, `fs`, `fs_lvm`, and `fs_lvm_ssh` as local-compatible backends.

### Local datastore placement guidance

OpenNebula local-style image datastores are best treated as placement-sensitive, not portable.

Recommended operator pattern for local-backed StorageClasses:

- use `volumeBindingMode: WaitForFirstConsumer`
- keep `compatibilityAwareSelection=true`
- enable `topologyAccessibility` only if you can maintain correct node labels
- use local-backed classes for node-sticky RWO workloads, not for workloads that must move freely between nodes

Important limitation:

- the driver does not perform CSI-side host-to-host data migration for local-backed PVCs
- when a pod lands on a different node, Kubernetes waits for detach/attach, but the underlying storage must already be attachable by OpenNebula from the configured image datastore path
- if OpenNebula cannot resolve the source image path during attach, the fix is in datastore layout or transfer-manager configuration, not in CSI-side file sync
- the controller serializes attach/detach/expand operations per volume, allows only one active VM hotplug per node at a time, and returns retryable `Aborted` for later same-node requests instead of letting them time out in-line
- hotplug attach, detach, and node-side device discovery now use a size-aware timeout budget derived from the actual OpenNebula disk size
- if a VM stays non-ready through the full hotplug timeout, the controller places that VM into a temporary recovery cooldown and rejects new hotplug work with retryable `Unavailable`
- recreating a MinIO tenant with local-backed PVCs should still be treated as node-sticky; if the workload must move freely across nodes after recreation, use Ceph RBD or CephFS instead

## Access mode support

Current access mode matrix:

- `ReadWriteOnce`: supported
- `ReadOnlyMany`: supported
- `ReadWriteMany`: supported for `Filesystem` volumes on the CephFS shared-filesystem path
- `MULTI_NODE_SINGLE_WRITER`: supported for `Filesystem` volumes on the CephFS shared-filesystem path
- `Block` + multi-node write: not supported

Routing is inferred from the requested access mode:

- `SINGLE_NODE_WRITER`, `SINGLE_NODE_READER_ONLY`, and `MULTI_NODE_READER_ONLY` stay on the OpenNebula disk/image path
- `MULTI_NODE_MULTI_WRITER` and `MULTI_NODE_SINGLE_WRITER` route to the CephFS shared-filesystem path

### Disk path vs shared-fs path

- Databases and other single-writer application data should still use the OpenNebula `IMAGE` datastore path
- Shared caches, model stores, and other multi-node RWX workloads can now use the CephFS shared-filesystem path
- `ReadOnlyMany` remains on the disk path in `v0.4.3`
- CephFS expansion is stable in `v0.4.3`
- CephFS snapshots and clones remain implemented behind feature gates and stay alpha-off by default in `v0.4.3`

## Ceph RBD support

Ceph support in this fork means OpenNebula Ceph **RBD** datastores. It does not include CephFS.

Current Ceph support matrix:

- Ceph Image Datastore + Ceph System Datastore
- Ceph Image Datastore + SSH System Datastore

The driver validates Ceph datastores before provisioning and also validates the target VM deployment mode before attach.

For Ceph Image Datastores the driver requires:

- `DS_MAD=ceph`
- `TM_MAD=ceph`
- `DISK_TYPE=RBD`
- `POOL_NAME`
- `CEPH_HOST`
- `CEPH_USER`
- `CEPH_SECRET`
- `BRIDGE_LIST`

For Ceph System Datastores in Ceph mode the driver requires:

- `TM_MAD=ceph`
- `DISK_TYPE=RBD`
- the same Ceph connection identity as the image datastore:
  `POOL_NAME`, `CEPH_HOST`, `CEPH_USER`, `CEPH_SECRET`
- if defined on both sides, `CEPH_CONF` and `CEPH_KEY` must also match

If the target VM is running on an SSH System Datastore, attach is allowed and the driver logs that OpenNebula SSH mode caveats apply.

### Ceph prerequisites

Operators must prepare the OpenNebula environment before using Ceph-backed StorageClasses:

- Ceph client tools must be installed on the OpenNebula frontend and hypervisor nodes.
- OpenNebula nodes using Ceph must be configured as Ceph clients.
- The libvirt secret referenced by `CEPH_SECRET` must exist on the hypervisor nodes.
- The required Ceph keyring/key material must be present where OpenNebula expects it.
- Ceph Image and System Datastores must be created in OpenNebula with the required attributes above.
- OpenNebula should inherit datastore attributes needed for VM disk deployment, typically:
  `CEPH_HOST`, `CEPH_SECRET`, `CEPH_USER`, `CEPH_CONF`, `POOL_NAME`, `DISK_TYPE`
- If you rely on formatted datablocks, the filesystem requested through `fsType` must be supported by the OpenNebula datastore drivers and `mkfs.<fs>` must be available on the relevant nodes.

Optional Ceph datastore attributes supported by the driver and documentation:

- `CEPH_CONF`
- `CEPH_KEY`
- `RBD_FORMAT`
- `EC_POOL_NAME`
- `CEPH_TRASH`

## CephFS RWX support

CephFS support in this fork is a SparkAI shared-filesystem path. It is separate from the existing OpenNebula image-backed disk path.

CephFS routing requirements:

- requested access mode must be `ReadWriteMany` or `MULTI_NODE_SINGLE_WRITER`
- volume capability must be `Filesystem`
- the selected datastore must resolve as `cephfs`

Required OpenNebula datastore template attributes for CephFS:

- `SPARKAI_CSI_SHARE_BACKEND=cephfs`
- `SPARKAI_CSI_CEPHFS_FS_NAME`
- `SPARKAI_CSI_CEPHFS_ROOT_PATH`
- `SPARKAI_CSI_CEPHFS_SUBVOLUME_GROUP`
- `CEPH_HOST`

Optional datastore template attributes:

- `CEPH_CONF`
- `SPARKAI_CSI_CEPHFS_MOUNT_OPTIONS`

CephFS StorageClass parameters:

- `sharedFilesystemPath`
  - if set, the claim is treated as a static existing CephFS path
  - if unset, the driver dynamically provisions a CephFS subvolume
- `sharedFilesystemSubvolumeGroup`
  - optional override for the datastore default

CephFS secret references:

- dynamic provisioning and deletion use the standard CSI provisioner secret reference
- CephFS expansion uses the standard CSI controller-expand secret reference
- node-side mounts use the standard CSI node-stage secret reference

Expected CephFS secret keys:

- provisioner secrets: `adminID`, `adminKey`
- node-stage secrets: `userID`, `userKey`

### CephFS prerequisites

- the selected datastore must be an OpenNebula `FILE` datastore
- the datastore template must expose the SparkAI CephFS attributes above
- Ceph monitor endpoints in `CEPH_HOST` must be reachable from the controller and node plugin pods
- the runtime image now includes `ceph`, `ceph-fuse`, and related Ceph packages
- CephFS node-stage auth is provided through Kubernetes Secret refs, not host-global Ceph config
- static CephFS paths configured through `sharedFilesystemPath` must already exist; the driver does not create them
- dynamic CephFS provisioning requires a Ceph user with permission to create, getpath, and remove subvolumes in the configured filesystem and subvolume group
- dynamic CephFS expansion also requires `csi.storage.k8s.io/controller-expand-secret-name` and `csi.storage.k8s.io/controller-expand-secret-namespace` on the StorageClass, typically pointing at the same secret used for the provisioner path
- CephFS expansion, snapshot, and clone flows require the corresponding feature gates to be enabled before use

### CephFS alpha feature gates

- `cephfsSnapshots=true` enables `CreateSnapshot`, `DeleteSnapshot`, and snapshot listing for dynamic CephFS subvolumes
- `cephfsClones=true` enables PVC clone from CephFS volumes and restore from CephFS snapshots
- `cephfsSelfHealing=true` enables one lazy-unmount/remount recovery attempt when node stage detects a stale CephFS mount during staging
- `NodeGetVolumeStats` reports stale or disconnected CephFS mounts as `FailedPrecondition` so kubelet surfaces a precise restage-needed error instead of a generic internal stat failure
- static CephFS paths created with `sharedFilesystemPath` still reject expansion

## Volume expansion

The chart supports `allowVolumeExpansion`, and the controller deployment now includes the CSI external resizer sidecar required by Kubernetes for PVC resize workflows.

Current behavior:

- Controller expansion is supported for OpenNebula volumes that are attached to a VM.
- Filesystem expansion is supported on the node for mounted filesystem volumes.
- Block volumes do not require node-side filesystem expansion.
- CephFS shared-filesystem volumes support expansion for dynamic subvolumes by default in `v0.4.3`.
- Static CephFS paths created with `sharedFilesystemPath` are not expandable.
- Detached-volume expansion is enabled by default in `v0.4.3` and uses image-level resize via `one.image.update`.
- Shrinking remains unsupported for disk and CephFS paths.

## Snapshots and clones

Stable disk-path data-management features in `v0.4.3`:

- CSI `CreateSnapshot`, `DeleteSnapshot`, and `ListSnapshots` for OpenNebula image-backed volumes
- PVC-to-PVC clone through CSI `VolumeContentSource.volume`
- the chart can deploy the external `csi-snapshotter` sidecar

Current limitations:

- restoring a new PVC from an OpenNebula image snapshot is not supported by the current backend path
- CephFS snapshots and clones remain alpha-off by default, but can be enabled with `cephfsSnapshots=true` and `cephfsClones=true`
- CephFS clone restore requires a dynamic CephFS volume source or a CephFS snapshot source

## Topology accessibility

Topology accessibility remains alpha and is disabled by default.

When `topologyAccessibility=true`:

- the plugin advertises CSI `VOLUME_ACCESSIBILITY_CONSTRAINTS`
- the node plugin reads the Kubernetes Node label `topology.opennebula.sparkaiur.io/system-ds`
- `NodeGetInfo` reports that label as the node’s accessible topology segment
- `CreateVolume` returns `accessible_topology` when the selected datastore exposes deterministic `COMPATIBLE_SYS_DS` values

Operational requirements:

- label Kubernetes nodes with `topology.opennebula.sparkaiur.io/system-ds=<opennebula-system-datastore-id>`
- ensure the node DaemonSet service account can `get` `nodes`
- if you need an override during testing, set `ONE_CSI_NODE_TOPOLOGY_SYSTEM_DS` through `driver.env`

Example objects:

- VolumeSnapshotClass: [examples/volumesnapshotclass.yaml](examples/volumesnapshotclass.yaml)
- VolumeSnapshot: [examples/volumesnapshot.yaml](examples/volumesnapshot.yaml)
- PVC clone: [examples/pvc-clone.yaml](examples/pvc-clone.yaml)

## Metrics and Prometheus

The Helm chart can now expose Prometheus-scrapable CSI sidecar metrics and driver-native metrics.

Sidecar metrics are exposed for:

- `csi-provisioner`
- `csi-attacher`
- `csi-resizer`
- `csi-node-driver-registrar`

These metrics cover CSI controller and node operations such as provisioning, attaching, resizing, request latency, and sidecar work-queue behavior.

Driver-native metrics add low-cardinality counters and gauges for:

- operation totals and latency
- datastore selection outcomes
- attach validation outcomes
- CephFS subvolume operations
- snapshot operations
- preflight checks
- datastore free and total bytes

Important distinction:

- PVC object metrics should come from `kube-state-metrics`
- CSI operation metrics come from this chart

Examples of PVC object metrics you should expect from `kube-state-metrics`:

- `kube_persistentvolumeclaim_info`
- `kube_persistentvolumeclaim_status_phase`
- `kube_persistentvolumeclaim_resource_requests_storage_bytes`

Chart metrics behavior:

- metrics endpoints are enabled by default
- the chart creates scrape `Service` objects for controller and node metrics
- optional `ServiceMonitor` objects can be enabled for Prometheus Operator environments
- standard Prometheus annotation-based scraping can also be used through `metrics.controller.service.annotations` and `metrics.node.service.annotations`

Relevant Helm values:

```yaml
metrics:
  enabled: true
  path: /metrics
  driver:
    enabled: true
    port: 9810
  controller:
    service:
      annotations: {}
      labels: {}
    serviceMonitor:
      enabled: false
      namespace: ""
      interval: 30s
      scrapeTimeout: 10s
      labels: {}
      annotations: {}
  node:
    service:
      annotations: {}
      labels: {}
    serviceMonitor:
      enabled: false
      namespace: ""
      interval: 30s
      scrapeTimeout: 10s
      labels: {}
      annotations: {}
```

If you are using Prometheus Operator, set:

```yaml
metrics:
  controller:
    serviceMonitor:
      enabled: true
  node:
    serviceMonitor:
      enabled: true
```

Example values enabling driver metrics and ServiceMonitors:

- [examples/driver-metrics-servicemonitor-values.yaml](examples/driver-metrics-servicemonitor-values.yaml)

## Preflight validation

The binary now supports a preflight mode that validates OpenNebula connectivity, datastore configuration, Ceph tooling, monitor reachability, referenced Kubernetes secrets, and optional CRD dependencies.

CLI example:

```bash
./opennebula-csi \
  --mode=preflight \
  --output=text \
  --preflight-datastores=111,300 \
  --preflight-node-stage-secrets=kube-system/cephfs-node-stage \
  --preflight-provisioner-secrets=kube-system/cephfs-provisioner \
  --require-snapshot-crds \
  --require-servicemonitor-crds
```

Helm can also run preflight as an optional Job, including as a release-blocking hook:

- [examples/helm-values-preflight.yaml](examples/helm-values-preflight.yaml)

## Feature gates

Stable features are enabled by default. Higher-risk features remain behind feature gates.

Current gates:

- `compatibilityAwareSelection=true`
- `detachedDiskExpansion=true`
- `cephfsExpansion=true`
- `cephfsSnapshots=false`
- `cephfsClones=false`
- `cephfsSelfHealing=false`
- `topologyAccessibility=false`

The chart renders these into `ONE_CSI_FEATURE_GATES`.

- `detachedDiskExpansion` enables image-level resize for detached persistent disks
- `cephfsExpansion` enables dynamic CephFS subvolume quota resize
- `cephfsSnapshots` enables CephFS snapshot RPCs
- `cephfsClones` enables CephFS PVC clone and snapshot restore flows
- `cephfsSelfHealing` controls stale CephFS remount attempts after a stale mount is detected during node stage
- `topologyAccessibility` enables node topology lookup and `accessible_topology` responses

## Helm installation

Add the SparkAI chart repo:

```bash
helm repo add sparkai-opennebula https://sparkaiur.github.io/storage-provider-opennebula/charts/
helm repo update
```

### Option 1: Existing Secret

Create a Secret with the OpenNebula credentials:

```bash
kubectl -n kube-system create secret generic opennebula-csi-auth \
  --from-literal=credentials='oneadmin:changeme'
```

Install the chart:

```bash
helm upgrade --install opennebula-csi sparkai-opennebula/opennebula-csi \
  --namespace kube-system \
  --create-namespace \
  --values examples/helm-values-existing-secret.yaml
```

### Option 2: Inline credentials

This is supported for convenience, but an existing Secret is the recommended production path.

```bash
helm upgrade --install opennebula-csi sparkai-opennebula/opennebula-csi \
  --namespace kube-system \
  --create-namespace \
  --values examples/helm-values-single-datastore.yaml
```

## Helm values model

The chart no longer renders a fixed set of StorageClasses. Instead it accepts a `storageClasses[]` list and flexible deployment settings.

### Key values

- `credentials.existingSecret.name`
- `credentials.existingSecret.key`
- `credentials.inlineAuth`
- `driver.defaultDatastores`
- `driver.datastoreSelectionPolicy`
- `driver.allowedDatastoreTypes`
- `driver.env`
- `controller.resources`
- `controller.nodeSelector`
- `controller.tolerations`
- `controller.affinity`
- `controller.podAnnotations`
- `controller.extraArgs`
- `controller.extraEnv`
- `node.resources`
- `node.nodeSelector`
- `node.tolerations`
- `node.affinity`
- `node.podAnnotations`
- `node.extraArgs`
- `node.extraEnv`
- `imagePullSecrets`
- `storageClasses[]`
- `featureGates`
- `metrics.driver`
- `preflight`
- `snapshotter`

### StorageClass schema

Each item under `storageClasses` supports:

- `name`
- `annotations`
- `labels`
- `reclaimPolicy`
- `allowVolumeExpansion`
- `mountOptions`
- `volumeBindingMode`
- `parameters`

## Example values files

- Single datastore by ID: [examples/helm-values-single-datastore.yaml](examples/helm-values-single-datastore.yaml)
- Multiple datastores with `least-used`: [examples/helm-values-multi-datastore.yaml](examples/helm-values-multi-datastore.yaml)
- Existing Secret with `default` alias: [examples/helm-values-existing-secret.yaml](examples/helm-values-existing-secret.yaml)
- Omni-oriented example: [examples/omni-values.yaml](examples/omni-values.yaml)
- Ceph RBD with Ceph System Datastore mode: [examples/helm-values-ceph-ceph-mode.yaml](examples/helm-values-ceph-ceph-mode.yaml)
- Ceph RBD with SSH System Datastore mode: [examples/helm-values-ceph-ssh-mode.yaml](examples/helm-values-ceph-ssh-mode.yaml)
- CephFS static RWX example: [examples/helm-values-cephfs-static.yaml](examples/helm-values-cephfs-static.yaml)
- CephFS dynamic RWX example: [examples/helm-values-cephfs-dynamic.yaml](examples/helm-values-cephfs-dynamic.yaml)
- Alpha feature-gate example: [examples/helm-values-feature-gates-alpha.yaml](examples/helm-values-feature-gates-alpha.yaml)
- Snapshotter/clone example values: [examples/helm-values-snapshotter.yaml](examples/helm-values-snapshotter.yaml)
- Preflight job example values: [examples/helm-values-preflight.yaml](examples/helm-values-preflight.yaml)
- CephFS node-stage secret example: [examples/cephfs-node-stage-secret.yaml](examples/cephfs-node-stage-secret.yaml)
- CephFS provisioner secret example: [examples/cephfs-provisioner-secret.yaml](examples/cephfs-provisioner-secret.yaml)
- Driver metrics + ServiceMonitor example values: [examples/driver-metrics-servicemonitor-values.yaml](examples/driver-metrics-servicemonitor-values.yaml)
- VolumeSnapshotClass example: [examples/volumesnapshotclass.yaml](examples/volumesnapshotclass.yaml)
- VolumeSnapshot example: [examples/volumesnapshot.yaml](examples/volumesnapshot.yaml)
- PVC clone example: [examples/pvc-clone.yaml](examples/pvc-clone.yaml)
- CephFS RWX demo workload: [examples/demo-busybox-cephfs-rwx.yaml](examples/demo-busybox-cephfs-rwx.yaml)
- Omni-oriented Ceph example: [examples/omni-values-ceph.yaml](examples/omni-values-ceph.yaml)
- Staging-lab Omni Ceph example: [examples/omni-values-staging-ceph.yaml](examples/omni-values-staging-ceph.yaml)

## Omni deployment notes

For Omni on OpenNebula, the common pattern is:

1. Create or reference an existing Secret with `ONE_AUTH`
2. Set `driver.defaultDatastores` to the datastores that Omni should consume
3. Create one or more StorageClasses under `storageClasses[]`
4. Use StorageClass-specific `parameters` to tune the underlying datastore behavior

The driver currently validates configured provisioning datastores against the allowed datastore type list, which defaults to `local,ceph,cephfs`.
For OpenNebula local-style datastores, the driver treats `local`, `fs`, `fs_lvm`, and `fs_lvm_ssh` as local-compatible.
Provisioning targets still need to be OpenNebula `IMAGE` or `FILE` datastores. `SYSTEM` datastores cannot be used for `CreateVolume`.
If you want PVC resizing in Omni, set `allowVolumeExpansion: true` on the relevant StorageClasses and ensure workloads are using attached volumes when expansion is requested.
If you need `ReadWriteMany`, use a CephFS-enabled StorageClass with node-stage and provisioner secrets wired in.

For Ceph-backed Omni deployments:

1. Create the OpenNebula Ceph Image Datastore and, if needed, the Ceph System Datastore.
2. Ensure Ceph prerequisites are satisfied on the frontend and hypervisor nodes.
3. Set `driver.defaultDatastores` or StorageClass `datastoreIDs` to the Ceph Image Datastore IDs.
4. Keep `driver.allowedDatastoreTypes` including `ceph`.
5. If workloads run in SSH System Datastore mode, expect OpenNebula SSH mode limitations for Ceph-backed images.

For CephFS-backed Omni deployments:

1. Create a `FILE` datastore in OpenNebula and add the SparkAI CephFS attributes to its template.
2. Set `driver.defaultDatastores` or StorageClass `datastoreIDs` to the CephFS datastore IDs.
3. Add node-stage and provisioner secret refs to the StorageClass parameters.
4. Use `ReadWriteMany` on the PVC to trigger the shared-filesystem path.
5. Validate the resulting deployment with [examples/demo-busybox-cephfs-rwx.yaml](examples/demo-busybox-cephfs-rwx.yaml).

## Staging validation gate

`v0.4.3` was validated on a live Omni + Talos + OpenNebula staging cluster before release. The repo includes:

- a local validation script: `hack/validate-staging-cephfs.sh`
- a manual GitHub Actions workflow: `.github/workflows/staging-cephfs-validation.yaml`
- the workflow builds and pushes an ephemeral `ghcr.io/sparkaiur/opennebula-csi:staging-<sha>` image before deploying, so staging always exercises the branch under test instead of the last tagged release

Local staging runs can override the deployed image with:

- `CSI_IMAGE_REPOSITORY`
- `CSI_IMAGE_TAG`

Required staging secrets for the workflow:

- `STAGING_KUBECONFIG_B64`
- `STAGING_ONE_XMLRPC`
- `STAGING_ONE_AUTH`
- `STAGING_CEPHFS_ADMIN_ID`
- `STAGING_CEPHFS_ADMIN_KEY`
- `STAGING_CEPHFS_NODE_USER_ID`
- `STAGING_CEPHFS_NODE_USER_KEY`

Sanitized staging-derived operator notes:

- local-style datastores should use `WaitForFirstConsumer` and be treated as placement-sensitive, not portable
- Ceph RBD volumes require the target VM to run on a compatible Ceph system datastore; a local `TM_MAD=local` system datastore is not a valid Ceph RBD attach target
- static CephFS paths must already exist before the claim is created
- dynamic CephFS expansion requires the standard CSI controller-expand secret refs on the StorageClass
- stale CephFS mounts surface as restage-needed errors in volume stats until the workload is restaged

## Local development

Build the binary:

```bash
go build ./cmd/opennebula-csi
```

Run the Go test suite:

```bash
go test ./...
```

Render the chart with example values:

```bash
helm lint helm/opennebula-csi
helm template opennebula-csi helm/opennebula-csi --values examples/helm-values-multi-datastore.yaml
helm template opennebula-csi helm/opennebula-csi --values examples/helm-values-ceph-ceph-mode.yaml
```

### Opt-in test suites

These suites are disabled by default because they require real infrastructure:

- `RUN_CSI_SANITY_TESTS=1 go test ./pkg/csi/driver -run TestDriver`
- `RUN_OPENNEBULA_INTEGRATION_TESTS=1 go test ./pkg/csi/opennebula`
- `RUN_OPENNEBULA_E2E_TESTS=1 go test ./pkg/csi/test/e2e`

## Release flow

Every semantic release must be validated in the `hplcsi` lab before the tag is created.

Required release gate:

1. Build the candidate image from the branch under test
2. Deploy that candidate image into the lab cluster, not the previous tagged release
3. Validate the feature area touched by the change on live infrastructure
4. Only after lab validation passes, create or move the semantic tag and publish the release

At minimum, release validation should include:

- `go test ./...`
- `helm template opennebula-csi ./helm/opennebula-csi ...`
- a live lab validation for the feature or hotfix being released

Push a semantic tag such as `v0.1.0` only after that validation to trigger the release workflow.

The workflow will:

1. Build and publish `ghcr.io/sparkaiur/opennebula-csi:<tag>` and `latest`
2. Build and publish `docker.io/nudevco/opennebula-csi:<tag>` and `latest`
3. Package the Helm chart
4. Publish the chart tarball and `index.yaml` to `gh-pages/charts/`
5. Re-index the Helm repo with `https://sparkaiur.github.io/storage-provider-opennebula/charts/`
6. Create a GitHub release for the tag

## Upstream

This project is derived from the OpenNebula storage provider. The fork keeps the Apache 2.0 license and focuses on SparkAI-specific OpenNebula and Omni requirements.
