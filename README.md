# SparkAI OpenNebula CSI

`storage-provider-opennebula` is the SparkAI fork of the OpenNebula CSI driver for Kubernetes. It publishes container images to `ghcr.io/sparkaiur/opennebula-csi:<tag>` and Helm charts to `https://sparkaiur.github.io/storage-provider-opennebula/charts/`.

This fork is focused on Omni deployments on OpenNebula and removes the old requirement that a datastore literally named `default` must exist for PVC provisioning.

## Current scope

- Provision PVCs on explicitly configured OpenNebula datastores.
- Support global driver datastore defaults and per-StorageClass overrides.
- Support `least-used` and `ordered` datastore selection policies.
- Support `Filesystem` and `Block` volume modes.
- Support `ReadWriteOnce` and `ReadOnlyMany` access modes.
- Support CSI volume expansion for attached volumes, including node-side filesystem growth.
- Support `local` and OpenNebula Ceph RBD datastores.
- Keep the internal selection/provider structure ready for future `cephfs` and `nfs` support.

## Release artifacts

- Container image: `ghcr.io/sparkaiur/opennebula-csi:<tag>`
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
- `ONE_CSI_ALLOWED_DATASTORE_TYPES`: CSV list, default `local,ceph`

### Policy behavior

- `least-used`: sort eligible datastores by free capacity descending, then try them in that order
- `ordered`: use the configured order as-is

If a candidate datastore does not have enough free capacity, the driver falls through to the next eligible datastore. If no configured datastore can satisfy the request, provisioning fails with a capacity error.

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

## Volume expansion

The chart supports `allowVolumeExpansion`, and the controller deployment now includes the CSI external resizer sidecar required by Kubernetes for PVC resize workflows.

Current behavior:

- Controller expansion is supported for OpenNebula volumes that are attached to a VM.
- Filesystem expansion is supported on the node for mounted filesystem volumes.
- Block volumes do not require node-side filesystem expansion.
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
- Omni-oriented Ceph example: [examples/omni-values-ceph.yaml](examples/omni-values-ceph.yaml)

## Omni deployment notes

For Omni on OpenNebula, the common pattern is:

1. Create or reference an existing Secret with `ONE_AUTH`
2. Set `driver.defaultDatastores` to the datastores that Omni should consume
3. Create one or more StorageClasses under `storageClasses[]`
4. Use StorageClass-specific `parameters` to tune the underlying datastore behavior

The driver currently validates configured provisioning datastores against the allowed datastore type list, which defaults to `local,ceph`.
If you want PVC resizing in Omni, set `allowVolumeExpansion: true` on the relevant StorageClasses and ensure workloads are using attached volumes when expansion is requested.

For Ceph-backed Omni deployments:

1. Create the OpenNebula Ceph Image Datastore and, if needed, the Ceph System Datastore.
2. Ensure Ceph prerequisites are satisfied on the frontend and hypervisor nodes.
3. Set `driver.defaultDatastores` or StorageClass `datastoreIDs` to the Ceph Image Datastore IDs.
4. Keep `driver.allowedDatastoreTypes` including `ceph`.
5. If workloads run in SSH System Datastore mode, expect OpenNebula SSH mode limitations for Ceph-backed images.

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
2. Package the Helm chart
3. Publish the chart tarball and `index.yaml` to `gh-pages/charts/`
4. Re-index the Helm repo with `https://sparkaiur.github.io/storage-provider-opennebula/charts/`
5. Create a GitHub release for the tag

## Upstream

This project is derived from the OpenNebula storage provider. The fork keeps the Apache 2.0 license and focuses on SparkAI-specific OpenNebula and Omni requirements.
