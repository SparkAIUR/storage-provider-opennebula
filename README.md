# SparkAI OpenNebula CSI

`storage-provider-opennebula` is the SparkAI fork of the OpenNebula CSI driver for Kubernetes. It publishes container images to `ghcr.io/sparkaiur/opennebula-csi:<tag>` and `docker.io/nudevco/opennebula-csi:<tag>`, and Helm charts to `https://sparkaiur.github.io/storage-provider-opennebula/charts/`.

This fork is focused on Omni deployments on OpenNebula and removes the old requirement that a datastore literally named `default` must exist for PVC provisioning.

## Current scope

- Provision PVCs on explicitly configured OpenNebula datastores.
- Support global driver datastore defaults and per-StorageClass overrides.
- Support `least-used` and `ordered` datastore selection policies.
- Support `Filesystem` and `Block` volume modes.
- Support `ReadWriteOnce`, `ReadOnlyMany`, and CephFS-backed `ReadWriteMany` filesystem volumes.
- Support CSI volume expansion for attached volumes, including node-side filesystem growth.
- Support `local`, OpenNebula Ceph RBD, and SparkAI CephFS datastores.
- Keep the internal selection/provider structure ready for future `nfs` support.

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
- `datastoreSelectionPolicy`: `least-used` or `ordered`
- `fsType`: filesystem hint for filesystem volumes

All other StorageClass parameters are passed through to the existing disk/image handling logic. That allows tuning values such as `devPrefix`, `cache`, `driver`, and similar OpenNebula disk options.

### Driver environment variables

- `ONE_CSI_DEFAULT_DATASTORES`: global CSV datastore list, for example `100,101`
- `ONE_CSI_DATASTORE_SELECTION_POLICY`: `least-used` or `ordered`
- `ONE_CSI_ALLOWED_DATASTORE_TYPES`: CSV list, default `local,ceph,cephfs`

### Policy behavior

- `least-used`: sort eligible datastores by free capacity descending, then try them in that order
- `ordered`: use the configured order as-is

If a candidate datastore does not have enough free capacity, the driver falls through to the next eligible datastore. If no configured datastore can satisfy the request, provisioning fails with a capacity error.

## Datastore category support

Provisioning targets must be OpenNebula datastore categories that can accept image allocation.

Current rules:

- `IMAGE`: supported for PVC provisioning
- `FILE`: supported for SparkAI CephFS RWX datastores when the datastore template exposes the required CephFS attributes
- `SYSTEM`: rejected for `CreateVolume`

For OpenNebula local-style datastores, the driver treats `local`, `fs`, `fs_lvm`, and `fs_lvm_ssh` as local-compatible backends.

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
- `ReadOnlyMany` remains on the disk path in `v0.4.0`
- CephFS expansion, snapshots, and clones are not implemented in `v0.4.0`

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

- dynamic provisioning uses standard CSI provisioner and controller-delete secret references
- node-side mounts use standard CSI node-stage secret references

Expected CephFS secret keys:

- provisioner/controller-delete secrets: `adminID`, `adminKey`
- node-stage secrets: `userID`, `userKey`

### CephFS prerequisites

- the selected datastore must be an OpenNebula `FILE` datastore
- the datastore template must expose the SparkAI CephFS attributes above
- Ceph monitor endpoints in `CEPH_HOST` must be reachable from the controller and node plugin pods
- the runtime image now includes `ceph`, `ceph-fuse`, and related Ceph packages
- CephFS node-stage auth is provided through Kubernetes Secret refs, not host-global Ceph config
- dynamic CephFS provisioning requires a Ceph user with permission to create, getpath, and remove subvolumes in the configured filesystem and subvolume group

## Volume expansion

The chart supports `allowVolumeExpansion`, and the controller deployment now includes the CSI external resizer sidecar required by Kubernetes for PVC resize workflows.

Current behavior:

- Controller expansion is supported for OpenNebula volumes that are attached to a VM.
- Filesystem expansion is supported on the node for mounted filesystem volumes.
- Block volumes do not require node-side filesystem expansion.
- CephFS shared-filesystem volumes do not support expansion in `v0.4.0`.
- Detached-volume expansion is not currently supported by the driver, because the OpenNebula API surface used by this fork only exposes a documented disk-resize path for attached VM disks.

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
- CephFS node-stage secret example: [examples/cephfs-node-stage-secret.yaml](examples/cephfs-node-stage-secret.yaml)
- CephFS provisioner secret example: [examples/cephfs-provisioner-secret.yaml](examples/cephfs-provisioner-secret.yaml)
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
3. Add node-stage and provisioner/controller-delete secret refs to the StorageClass parameters.
4. Use `ReadWriteMany` on the PVC to trigger the shared-filesystem path.
5. Validate the resulting deployment with [examples/demo-busybox-cephfs-rwx.yaml](examples/demo-busybox-cephfs-rwx.yaml).

## Staging validation gate

`v0.4.0` is intentionally blocked on a live CephFS staging validation pass. The repo now includes:

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

For the SparkAI staging lab on `on.lab.sprkinfra.com`, use the dedicated example at `examples/omni-values-staging-ceph.yaml` and point `datastoreIDs` at the validated `one-csi` Ceph-backed datastore exposed by the staging OpenNebula frontend.

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

Push a semantic tag such as `v0.1.0` to trigger the release workflow.

The workflow will:

1. Build and publish `ghcr.io/sparkaiur/opennebula-csi:<tag>` and `latest`
2. Build and publish `docker.io/nudevco/opennebula-csi:<tag>` and `latest`
3. Package the Helm chart
4. Publish the chart tarball and `index.yaml` to `gh-pages/charts/`
5. Re-index the Helm repo with `https://sparkaiur.github.io/storage-provider-opennebula/charts/`
6. Create a GitHub release for the tag

## Upstream

This project is derived from the OpenNebula storage provider. The fork keeps the Apache 2.0 license and focuses on SparkAI-specific OpenNebula and Omni requirements.
