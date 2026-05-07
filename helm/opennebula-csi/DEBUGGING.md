# Local RWO Annotation Debugging Guide

This guide is for operators debugging local single-writer OpenNebula volumes in the `v0.5.x` line.

It focuses on the driver-owned annotation contract under:

- `storage-provider.opennebula.sparkaiur.io/*`

The goal is to let you:

- understand what the driver thinks the placement state is
- bias or pin a local `ReadWriteOnce` volume to a node during investigation
- authorize a bounded cross-node move when you have external evidence it is safe
- remove the debugging hints cleanly after remediation

## Scope

These annotations matter only for local, single-writer, non-shared volumes. They are ignored for:

- Ceph RBD portability workflows
- CephFS shared-filesystem volumes
- shared or multi-writer access modes

For local RWO, treat them as a debugging and recovery interface, not as a general scheduling API.

## Hard vs Soft Hints

The most important distinction:

- `required-node` is hard and fail-closed
- `preferred-node` is soft and best-effort
- historical `last-attached-node` is also soft and best-effort

Hard means:

- the admission webhook injects required hostname affinity
- controller publish to any other node is rejected with retryable `Unavailable`
- invalid or incompatible targets fail closed

Soft means:

- the webhook may inject preferred affinity
- the controller does not treat it as a repair bypass
- stale or incompatible hints are downgraded to warnings and skipped

## Operator Annotations

### `required-node`

Use when you need a strict node target during debugging or manual recovery.

Example:

```yaml
metadata:
  annotations:
    storage-provider.opennebula.sparkaiur.io/required-node: "bravo-worker-02"
```

Use this when:

- you know the disk must remain on a specific node
- you are recovering a StatefulSet pod and want any wrong-node scheduling to fail fast
- you want the webhook and controller to enforce the same target

The driver validates:

- Kubernetes node existence
- OpenNebula inventory presence when inventory is enabled
- compatible system-datastore topology for the volume

If validation fails, the request fails closed.

### `required-node-until`

Use with `required-node` when you want the strict target to expire automatically.

Example:

```yaml
metadata:
  annotations:
    storage-provider.opennebula.sparkaiur.io/required-node: "bravo-worker-02"
    storage-provider.opennebula.sparkaiur.io/required-node-until: "2026-05-05T22:00:00Z"
```

Behavior:

- format must be RFC3339 UTC
- after expiry, the annotation is ignored
- the driver does not auto-remove it
- support output and events show that it expired

Use this when you want a bounded safety window instead of a permanent pin.

### `preferred-node`

Use when you want to bias scheduling toward a node without turning it into a hard dependency.

Example:

```yaml
metadata:
  annotations:
    storage-provider.opennebula.sparkaiur.io/preferred-node: "bravo-worker-02"
```

Behavior:

- the webhook injects preferred hostname affinity
- if the node is stale, missing, tombstoned, or topology-incompatible, the driver warns and skips it
- this never fails pod admission by itself

Use this first when you are investigating and do not yet want to hard-pin the workload.

### `placement-reason`

Use for operator breadcrumbs.

Example:

```yaml
metadata:
  annotations:
    storage-provider.opennebula.sparkaiur.io/preferred-node: "bravo-worker-02"
    storage-provider.opennebula.sparkaiur.io/placement-reason: "minio-recovery-after-node-reboot"
```

This does not change behavior. It exists so support output and future operators can understand why a hint was added.

### `allow-cross-node-until`

This is the emergency override for protected local RWO movement.

Example:

```yaml
metadata:
  annotations:
    storage-provider.opennebula.sparkaiur.io/allow-cross-node-until: "2026-05-05T22:00:00Z"
```

Use this only when:

- the driver reports a protected historical node
- you have external evidence that a bounded cross-node move is safe

This override:

- can authorize an explicit `required-node` that conflicts with inferred protected ownership
- does not bypass topology validation
- does not bypass repair-required states
- does not bypass quarantine

If the volume is blocked by `wrong_device_identity`, metadata drift, host-artifact quarantine, or similar repair-required state, this annotation is not enough.

## Where To Apply Annotations

### PVC vs PV

The driver resolves placement in this order:

1. PV `required-node`
2. PVC `required-node`
3. controller-inferred protected node
4. PV `preferred-node`
5. PVC `preferred-node`
6. PV `last-attached-node`
7. deprecated PV `preferred-last-node`

Use PVC annotations when:

- you are debugging from the workload side
- you want the hint to live with the claim

Use PV annotations when:

- you want the strongest precedence
- you are intervening directly on a bound existing volume
- you need the emergency cross-node override

## Internal Pod Annotations

When the webhook mutates a pod, it writes internal breadcrumbs:

- `storage-provider.opennebula.sparkaiur.io/required-node-injected`
- `storage-provider.opennebula.sparkaiur.io/preferred-node-injected`
- `storage-provider.opennebula.sparkaiur.io/placement-source`
- `storage-provider.opennebula.sparkaiur.io/placement-decision`

These are not operator inputs. They are evidence of what the webhook decided.

Useful interpretations:

- `placement-decision=required`
  means hard placement was enforced
- `placement-decision=preferred`
  means soft placement was injected
- missing injected annotations with warnings
  usually means the hint was stale or incompatible and got skipped

## Typical Workflows

### 1. Bias a restart back to the historical node

Start with a soft hint:

```bash
kubectl -n <ns> annotate pvc <pvc> \
  storage-provider.opennebula.sparkaiur.io/preferred-node=bravo-worker-02 \
  storage-provider.opennebula.sparkaiur.io/placement-reason='restart-debug'
```

Then recreate the pod and inspect:

```bash
kubectl -n <ns> get pod <pod> -o yaml
```

Look for:

- `preferred-node-injected`
- `placement-source`
- `placement-decision=preferred`

If the node is stale or incompatible, the pod should still admit, but the warning path will show that the hint was skipped.

### 2. Hard-pin a local RWO volume during manual recovery

When soft placement is not enough:

```bash
kubectl annotate pv <pv> \
  storage-provider.opennebula.sparkaiur.io/required-node=bravo-worker-02 \
  storage-provider.opennebula.sparkaiur.io/required-node-until=2026-05-05T22:00:00Z \
  storage-provider.opennebula.sparkaiur.io/placement-reason='manual-minio-recovery'
```

Expected behavior:

- pod admission injects required affinity
- wrong-node publishes are rejected
- invalid node targets fail immediately instead of degrading silently

Use this when you need the system to tell you quickly that the recovery assumptions are wrong.

### 3. Authorize a bounded protected move

If the driver says the volume is still historically protected to node A, but you have external repair evidence for node B:

```bash
kubectl annotate pv <pv> \
  storage-provider.opennebula.sparkaiur.io/required-node=bravo-worker-03 \
  storage-provider.opennebula.sparkaiur.io/allow-cross-node-until=2026-05-05T22:00:00Z \
  storage-provider.opennebula.sparkaiur.io/placement-reason='operator-approved-move'
```

This is the strongest debugging intervention short of external storage repair. Use it intentionally and remove it after the move succeeds.

### 4. Clear debugging hints after success

After the workload is stable and healthy:

```bash
kubectl annotate pvc <pvc> \
  storage-provider.opennebula.sparkaiur.io/preferred-node- \
  storage-provider.opennebula.sparkaiur.io/placement-reason-
```

```bash
kubectl annotate pv <pv> \
  storage-provider.opennebula.sparkaiur.io/required-node- \
  storage-provider.opennebula.sparkaiur.io/required-node-until- \
  storage-provider.opennebula.sparkaiur.io/allow-cross-node-until- \
  storage-provider.opennebula.sparkaiur.io/placement-reason-
```

Do not leave old debugging hints behind unless they are part of a deliberate operational policy.

## What To Inspect When It Does Not Work

### Pod mutation result

Inspect the created pod:

```bash
kubectl -n <ns> get pod <pod> -o yaml
```

Check:

- `metadata.annotations`
- `spec.affinity`
- existing `nodeSelector` or required node affinity that may already conflict

### PV and PVC annotations

Inspect both objects together:

```bash
kubectl get pv <pv> -o yaml
kubectl -n <ns> get pvc <pvc> -o yaml
```

This matters because PV annotations beat PVC annotations for both `required-node` and `preferred-node`.

### Support bundle and volume health

Use the standard operator outputs:

```bash
opennebula-csi --mode=support-bundle
opennebula-csi --mode=volume-health
```

Fields to inspect:

- `lastNodeProtection`
- `annotationAudit`
- `volumeDemand`
- `volumeHistory`
- `volumeRepairState`

Important interpretations:

- `Invalid=true` on `lastNodeProtection`
  usually means hard `required-node` failed validation or conflicted with protected ownership
- warnings mentioning `soft preferred-node was ignored`
  mean the hint was best-effort and got skipped
- warnings mentioning `historical last-attached-node was ignored`
  mean stale history was detected and correctly downgraded
- annotation audit finding on `last-attached-node`
  means the historical hint is stale and should not be trusted

### Legacy local-RWO history bootstrap

Older local RWO volumes may predate durable `volumeHistory`.
If a local PV/PVC still has Kubernetes-side ownership evidence, the controller now
bootstraps a clearly marked history record from runtime metadata before reducing
an attach failure to `NotFound`.

Bootstrapped evidence can come from:

- PV/PVC `storage-provider.opennebula.sparkaiur.io/last-attached-node`
- PV/PVC `placement-summary.lastAttachedNode`
- local backend, datastore, PVC/PV identity, and restart-optimization annotations

Important interpretations:

- `volumeHistory[*].evidenceSource=bootstrap_runtime_annotations` means the record
  came from Kubernetes runtime metadata, not a successful publish/stage observation.
- `volumeHistory[*].bootstrapped=true` must have zero
  `lastSuccessfulPublishTime` and zero `lastSuccessfulStageTime`.
- `volumeRepairState[*].classification=historical_node_tombstone` means the
  last historical node no longer exists or has a mismatched UID.
- `volumeRepairState[*].classification=missing_image_record` means the local
  volume has historical local ownership evidence, but the provider-side image
  record/source is missing.

Truly unknown volumes, non-local volumes, or local volumes without historical
ownership evidence should still return ordinary `volume not found`.

### Orphan teardown and stale historical protection

If the workload is already gone, but the final detach still appears stuck on historical local-RWO ownership:

- inspect `volumeDemand` first
- check whether the PV is `Released`, deleting, or no longer bound to a live claim
- check whether the remaining `VolumeAttachment` is already deleting

In `v0.5.x`, orphan teardown now prefers detach progress over stale historical ownership. The controller should no longer need a restart just to let a deleting local-RWO attachment finish tearing down.

If it still does not move, verify that the blocker is not one of the stronger classes that still intentionally win:

- active `recovery-mode=manual`
- repair-required state
- volume quarantine
- host-artifact quarantine

### Refresh stale history-backed protection

If `lastNodeProtection` still points at an old historical node after you already proved that history is stale:

1. inspect `volumeHistory` in the support bundle
2. remove or edit the single stale key in `ConfigMap/opennebula-csi-volume-history-state`
3. trigger a fresh controller evaluation by retrying the publish/detach path or re-running the support bundle

You no longer need a controller restart just to invalidate one stale `volumeHistory` entry. The controller now refreshes that entry directly on:

- degraded local-RWO protection evaluation
- publish lookup error handling
- orphan detach validation

Confirm the refresh by checking that:

- `volumeHistory` no longer contains the stale record
- `lastNodeProtection` stops reporting `historical_ownership_active` for that volume
- orphan detach validation no longer pauses forever on the old node

### Events

Inspect workload and CSI namespace events:

```bash
kubectl -n <ns> get events --sort-by=.lastTimestamp
kubectl -n kube-system get events --sort-by=.lastTimestamp
```

Useful reasons:

- `RequiredNodeBlocked`
- `RequiredNodeExpired`
- `RequiredNodeConflict`
- `RequiredNodeInvalid`
- `MaintenanceHintIgnored`

### Manual maintenance safety

During maintenance mode, the driver now validates node hints before preserving them forward.

That means:

- a stale `preferred-node` should be ignored rather than copied into `last-attached-node`
- a stale historical `last-attached-node` should not become the protected node by itself
- live attachment and sticky evidence still matter, but invalid node names should no longer deadlock scheduling

If maintenance behavior looks wrong, inspect:

- the current PV `last-attached-node`
- sticky attachment state in the support bundle
- `MaintenanceHintIgnored` events
- whether the referenced node still exists in Kubernetes and inventory

## MinIO Recovery Guidance

These learnings come from recovering a degraded local-RWO MinIO tenant after Talos rollout churn caused repeated attach/detach storms, stale `VolumeAttachment` state, and OpenNebula runtime drift.

Use this as an operator playbook when a MinIO StatefulSet ordinal is stuck or comes back with the wrong shard.

### Preserve the blast radius first

For MinIO recovery:

- recover one tenant and one ordinal at a time
- keep unrelated MinIO pools or namespaces scaled down while you are proving storage state
- avoid cross-node movement until you know which node and which backing disk are correct

Do not let Kubernetes keep “trying things” against local RWO storage while you are still discovering the actual disk state.

### Prefer same-node recovery before any cross-node move

When a MinIO pod previously had valid data on node A:

1. try to keep the ordinal on node A
2. start with `preferred-node`
3. escalate to `required-node` only when you need fail-closed behavior
4. use `allow-cross-node-until` only after external storage evidence proves a bounded move is safe

In practice, this often means:

- cordoning competing nodes temporarily
- checking the pod, PVC, PV, and `VolumeAttachment` together
- validating that the OpenNebula VM attachment and the Kubernetes scheduling target still match

### Distinguish the failure class before you repair

Do not treat all MinIO attach failures as the same issue.

#### 1. Stale API or stale external-attacher work

Symptoms:

- `VolumeAttachment` is already gone or no longer reflects the intended state
- pod still times out waiting for the external attacher
- controller appears to be acting on stale in-memory detach or attach work

Interpretation:

- this is controller-side reconciliation drift, not necessarily a bad disk

Guidance:

- confirm the API state first
- avoid changing MinIO rollout state until CSI controller state is coherent
- if the API is clean but the sidecar is still acting on dead work, a bounded controller restart may be the cleanest operational reset

#### 2. OpenNebula metadata says attached, but the guest runtime is missing the disk

Symptoms:

- OpenNebula or VM template shows the disk attached
- kubelet or node-stage waits on `/dev/sdX` or by-id resolution
- libvirt runtime or guest device list does not actually contain the expected disk

Interpretation:

- this is a same-node runtime attach problem
- do not solve it with a cross-node move

Guidance:

- inspect the live VM runtime attachment, target, and serial
- repair the same-node hotplug/runtime visibility first
- only after the device is visible on the correct node should you continue with staging and pod startup

#### 3. The disk mounts, but MinIO sees the wrong shard identity

Symptoms:

- MinIO reports one drive offline or wrong while Linux mount looks healthy
- `.minio.sys/format.json` shows duplicate `this` values across different exports
- an expected MinIO disk UUID is missing from the set

Interpretation:

- this is not a scheduler problem anymore
- it usually means a replacement/empty disk or wrong historical disk landed in the slot

Guidance:

- stop trusting heal to fix it
- preserve current state before writing anything else
- recover the original shard from OpenNebula/LV artifacts if possible
- restore it into the exact original slot before resuming heal

### 4. The controller is fighting recovery

Symptoms:

- you delete a stale host-artifact or quarantine key and the same conflict appears to keep winning immediately
- `VolumeAttachment` API state looks cleaner than the controller behavior
- an external-attacher timeout or stale same-node detach keeps blocking fresh attach progress
- the correct disk is already present on-node, but the CSI control plane still refuses to let kubelet proceed

Interpretation:

- this is a recovery-control problem, not just a storage problem
- deleting ConfigMap keys alone may not be enough if the controller is still holding equivalent in-memory state

Guidance:

1. Confirm current API truth first:
   - `VolumeAttachment`
   - host-artifact state
   - quarantine state
   - desired workload placement
2. Confirm the data-plane truth separately:
   - OpenNebula VM template/runtime state
   - actual guest-visible device by serial or `/dev/disk/by-id`
   - current kubelet stage path and mount source
3. If the API is clean but the controller is clearly still acting on dead state, a bounded restart of `opennebula-csi-controller-0` is an acceptable incident workaround.
4. Do not treat direct `VolumeAttachment.status` patching as the desired steady-state operator interface. It is a bounded recovery bypass for cases where:
   - the correct device is already attached on the intended node
   - you have proven the serial and guest-visible device identity
   - the normal attach path is blocked by a known controller or OpenNebula bug
5. Keep proof or anchor pods in place until the consuming workload is actually `Running`, so kubelet does not immediately re-enter the same bad attach path.

If you repeatedly need controller restarts or direct `VolumeAttachment.status` surgery, treat that as a product gap and capture it for CSI follow-up rather than normalizing it as standard workflow.

### 5. Use recovery-mode before you restart the controller

The supported control-plane interface for one affected local RWO volume is now:

- `storage-provider.opennebula.sparkaiur.io/recovery-mode`
- `storage-provider.opennebula.sparkaiur.io/recovery-mode-until`
- `storage-provider.opennebula.sparkaiur.io/recovery-ticket`
- `storage-provider.opennebula.sparkaiur.io/adopt-attached-device`
- `storage-provider.opennebula.sparkaiur.io/confirmed-device-serial`
- `storage-provider.opennebula.sparkaiur.io/confirmed-volume-name`

Use them on the PV or PVC. PV wins if both are set.

Recommended first move when the controller is fighting recovery:

```bash
kubectl annotate pv <pv> \
  storage-provider.opennebula.sparkaiur.io/recovery-mode=manual \
  storage-provider.opennebula.sparkaiur.io/recovery-mode-until=2026-05-06T04:00:00Z \
  storage-provider.opennebula.sparkaiur.io/recovery-ticket='incident-1234'
```

Effect:

- attach and detach churn is suppressed for that volume only
- stale host-artifact or metadata-drift memory no longer requires a controller restart just to stop the fight
- diagnostics continue updating

Use `recovery-mode=observe` when you want the driver to keep reporting fresh evidence while suppressing new repair-driving actions, but you are not ready to force a manual workflow yet.

Use `recovery-mode=disabled` to drop the suppression and return the volume to normal control-plane behavior.

### 6. Supported adopt-attached-device workflow

This replaces direct `VolumeAttachment.status` patching as the preferred steady-state recovery interface.

Use it only after you have independent node evidence for:

- the exact guest-visible device name
- the exact `onecsi-<imageID>` serial
- the target node that still genuinely desires the workload

Example:

```bash
kubectl annotate pv <pv> \
  storage-provider.opennebula.sparkaiur.io/recovery-mode=manual \
  storage-provider.opennebula.sparkaiur.io/recovery-mode-until=2026-05-06T04:00:00Z \
  storage-provider.opennebula.sparkaiur.io/recovery-ticket='incident-1234' \
  storage-provider.opennebula.sparkaiur.io/adopt-attached-device=true \
  storage-provider.opennebula.sparkaiur.io/confirmed-device-serial=onecsi-394 \
  storage-provider.opennebula.sparkaiur.io/confirmed-volume-name=sdf
```

What the controller now does:

- confirms the volume is still desired on that node
- confirms the serial matches the expected volume identity
- refuses the adoption if OpenNebula still shows a conflicting owner
- returns publish context using the operator-confirmed guest-visible device instead of forcing unsupported `VolumeAttachment.status` surgery

This fails closed if:

- the desired node moved
- the confirmed serial is wrong
- the guest-visible device name is guessed
- OpenNebula still shows another owner

### 7. When a controller restart is still acceptable

A targeted restart of `opennebula-csi-controller-0` is still an incident workaround when:

- you have already cleared or invalidated the per-volume recovery annotations
- the API truth is clean
- support-bundle recovery state still shows no active suppression, but behavior is obviously stale

It is a workaround, not the primary interface.

Treat repeated restarts as a product gap when:

- the same volume repeatedly needs a restart after bounded recovery annotations were used correctly
- the support bundle keeps showing stale in-memory queue, quarantine, or host-artifact blockers after the external state is clean
- the only path that works is direct `VolumeAttachment.status` editing

### 8. Supported node-side unstage / reprobe

When the stage path itself is stale or contaminated on the node, use the supported node-side reprobe action from the node plugin pod on the affected node:

```bash
opennebula-csi -mode=local-disk-reprobe -volume-id <volume-id>
```

What it does:

- targets one local-RWO volume only
- cleans that volume's stage path with the same unstage path used by the node server
- removes the persisted node-local session for that volume
- leaves before/after evidence in the command output so you can see what changed

If the volume still has published consumer targets, the command fails closed by default.

To force the unstage anyway, first put the volume into bounded manual recovery mode and then acknowledge the risk explicitly:

```bash
opennebula-csi -mode=local-disk-reprobe -volume-id <volume-id> -allow-published-reprobe
```

That forced path is accepted only when `recovery-mode=manual` is already active for the volume. It is meant for targeted incident work, not routine operations.

Use this when:

- `nodeLocalDiskSessions` shows a stale `globalmount`
- the current stage mount source does not match the expected device evidence
- you need the next `NodeStageVolume` to perform a fresh node-side reprobe instead of reusing contaminated local session state

### 9. What to inspect now

For a current incident, collect:

- `opennebula-csi --mode=support-bundle`
- `opennebula-csi --mode=local-disk-sessions` from the node plugin pod on the affected node
- `opennebula-csi -mode=local-disk-reprobe -volume-id <volume-id>` when you intentionally need a single-volume node-side reset

The relevant fields are now:

- `volumeRecoveryControl`
- `localDeviceReports`
- `nodeLocalDiskSessions`
- `volumeQuarantine`
- `hostArtifactQuarantine`
- `hotplugQueue`

Focus on:

- recovery-mode status and expiry
- recovery ticket
- whether the volume is in adopted/manual state
- expected serial versus actual guest-visible device
- current stage-path mount source and whether it matches the expected device evidence

### Validate MinIO shard identity, not just mount success

For local RWO MinIO, a successful mount is necessary but not sufficient.

Before you trust a recovered ordinal:

- inspect `.minio.sys/format.json` on every export
- confirm the erasure set contains the expected number of unique `this` disk IDs
- look for duplicates, missing IDs, or obviously fresh metadata-only disks
- compare approximate disk usage between exports to distinguish real shards from empty replacements

If the disk identity is wrong, do not keep healing or writing through that slot.

### Use MinIO object visibility as the source of truth

During repair, top-level MinIO totals can lag or mislead.

Prefer:

- `mc admin info <alias>` for drive and erasure-set health
- `mc admin heal <alias>` for active repair progress
- `mc ls --recursive --summarize <alias>/<bucket>` for bucket-level readability

Do not assume low object counts in `mc admin info` always mean permanent loss while healing is still in progress. Verify with direct bucket listing and shard inspection.

### Recovery sequencing that worked

The successful pattern was:

1. identify which currently mounted exports were original and which were empty replacements
2. recover an original shard from OpenNebula/LV artifacts
3. place that shard back into its exact logical slot
4. keep the affected ordinal on the proven node
5. let MinIO heal only after quorum of original shard identities was restored

The key point is sequencing:

- restore the correct disk first
- heal second

Not the other way around.

### Cleanup after a successful MinIO repair

After the tenant is healthy:

- remove `required-node`, `required-node-until`, and `allow-cross-node-until`
- remove temporary `preferred-node` hints if they were only for incident handling
- uncordon temporary node blocks only after repeated restarts prove the attach path is stable
- capture support-bundle and volume-health output while the system is healthy, so future incidents have a known-good baseline
- if you used proof pods, manual runtime hotplug, or a bounded `VolumeAttachment` recovery bypass, remove those only after:
  - heal has progressed sufficiently
  - bucket-level reads look correct
  - at least one controlled restart test passes without reintroducing stale-stage or wrong-device behavior

### What not to do

Avoid these failure-amplifying patterns:

- do not let multiple MinIO ordinals flap across nodes while OpenNebula disk ownership is still unclear
- do not use manual node annotations to bypass `wrong_device_identity`, metadata drift, or quarantine
- do not trust an empty replacement PVC just because it stages successfully
- do not edit MinIO `format.json` on a live disk to “make the set green” unless you have already proven the underlying shard data is correct
- do not turn on `mc mirror --remove` or similar destructive sync behavior while the source tenant is still healing

## Deprecated Annotation

`storage-provider.opennebula.sparkaiur.io/preferred-last-node` is deprecated.

Current behavior in `v0.5.x`:

- still read as a fallback on PVs
- warning surfaced in annotation audit
- should be replaced with `preferred-node`

Do not introduce new workflows that depend on it.

## Practical Guidance

Use this order of operations:

1. Start with observation: inspect support output and current pod/PV/PVC state.
2. Try `preferred-node` first when you want to bias, not force.
3. Use `required-node` only when you want the system to fail closed around a known-safe node.
4. Add `required-node-until` for bounded interventions.
5. Use `allow-cross-node-until` only when you are intentionally overriding protected ownership.
6. Remove the annotations after the recovery window ends.

If you are uncertain whether the data path is actually portable, do not use annotations to guess around the storage truth. Repair-required states still need real remediation.
