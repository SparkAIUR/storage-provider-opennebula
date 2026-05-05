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
