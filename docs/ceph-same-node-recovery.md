# Ceph same-node attachment recovery

This source correction addresses a restart loop in which a replacement Pod is scheduled on the original node, its Ceph RBD disk is still attached to that VM, and the previous `VolumeAttachment` is deleting. The queue previously rejected detach because Kubernetes still desired the volume there; Kubernetes could not complete publication while that attachment object was deleting. Disabling the queue also produced misleading safe-detach history for a retained disk.

## Automatic retention contract

The controller can acknowledge logical deletion of that attachment while retaining the existing disk on the same VM. This permits external-attacher to remove its finalizer normally and Kubernetes to create a replacement attachment. The correction does not remove finalizers itself.

Automatic acknowledgement requires all of the following:

- A bound, non-deleting PV/PVC pair with matching claim UID, CSI driver and volume handle, matching RWO or RWOP access modes, and equal completed requested/PVC/PV capacities.
- A Ready node with the same UID and image identity recorded in persisted successful publish or stage history, without bootstrap-only provenance, one owned deleting attached VolumeAttachment, and current non-terminating consumers scheduled only on that node. RWOP additionally requires one consumer.
- Two read-only provider observations agreeing on the sole VM owner, image, disk ID, target, serial and completed capacity. The image must be persistent and stably in use; the disk must use Ceph, `CLONE=NO`, `PERSISTENT=YES` and `LN_TARGET=NONE`. Pending resize markers, unstable VM state, unknown ownership or capacity disagreement prevent acknowledgement.
- Fresh identity, demand, history and control checks after the provider observations. Active observation/recovery controls, repair state, quarantines and unreadable or malformed protection state prevent automatic retention. Explicit manual recovery retains its existing separate handler and semantics.
- The attachment reconciler must be enabled so a retained disk remains discoverable and eligible for ordinary orphan cleanup.

The automatic path performs no physical attach, detach, resize, restore or delete operation. It does not record a physical safe detach, update successful-attachment history, or clear quarantine/history. Applicable incomplete or conflicting evidence returns an actionable failure; it does not fall through to the old queue-disabled acknowledgement. Local maintenance/sticky policy, CephFS handling and explicit manual recovery are unchanged.

## Ownership and cleanup

The durable ownership source is OpenNebula's VM disk inventory, which the attachment reconciler reads independently of VolumeAttachment objects. If the old attachment disappears and no replacement publishes, the controller can restart, reconstruct ownership from that backend inventory and persisted Kubernetes state, wait the orphan grace period (120 seconds by default), and attempt ordinary detach only after current demand/protection checks allow it. No new in-memory hold or fabricated absence is needed. A delayed old Unpublish RPC cannot use a newly published non-deleting VolumeAttachment as authority for automatic acknowledgement or physical detach even after the consumer terminates or disappears.

Cleanup is bounded by the lifetime of the owned PV identity. The reconciler associates inventory only with this driver's PVs and VolumeAttachments, starts orphan grace again for a different PV UID, and cannot recover an inventory-to-claim association after the PV is removed. Keep the PV and its normal deletion finalizer until physical ownership is authoritatively resolved. Ordinary CSI DeleteVolume refuses an image still owned by a VM, an offline or unreadable ownership observation, and inconsistent ownership records. It reports deletion success only after the provider establishes absence or completes ordinary deletion and observes image absence. Logical retention does not release physical ownership or authorize the PV finalizer to complete early.

Physical cleanup rechecks recovery controls, repair and quarantine state, current Pod references including terminating and terminal consumers, node UID/readiness and node volumesInUse, and current attachment deletion authority at the action boundary. Missing release observations block cleanup. Stale VolumeAttachment deletion uses UID-scoped grace, fresh PV/VA and provider observations under node-then-volume locks, and UID/resourceVersion deletion preconditions. Skipped, unknown and already-absent cleanup attempts do not emit successful-detach events; a physical detach must pass provider readback before reconciliation reports it as detached.

PV/PVC UID comparisons establish the current binding and detect changes between observations. Existing history records store image/node pins and PV/PVC names, rather than historical PV/PVC UIDs; the controller does not claim to detect a prior same-image static rebind before its first read. Every available history and VolumeAttachment image, serial, target or claim assertion must agree with the current binding and device.

The two provider/Kubernetes observations are bounded checks, not atomic provider fencing. Controller locks serialize this controller's operations; they do not prevent an external OpenNebula actor from changing ownership after the last read. Same-node physical retention does not prove an old process stopped, inspect guest mounts, revoke writer access or fence database writers. Database/operator fencing and the ordinary publish/node-stage identity checks remain necessary. Pending or terminal Pod phase alone is not proof that a disk is unmounted.

## Validation and rollout

Public CSI regressions cover both queue modes, positive retained ownership, history preservation, unknown and conflicting evidence, late identity/control changes, cancellation, explicit manual behavior, delayed old RPCs, and restart/orphan cleanup without another Unpublish request. Provider protocol tests exercise actual XML-RPC parsing and assert zero mutations for positive and denied observations.

These local tests are separate from publication and hplzdev runtime acceptance. Validate a new immutable candidate on disposable hplzdev Ceph volumes, including checksum reads during deletion/republication, ordinary cleanup and preservation of existing resources. Do not reuse earlier RC3 receipts as proof for the new candidate. This correction does not qualify OpenNebula 7.4.0 persistent SYSTEM-target LVM expansion or waive the existing production rollout/observation gates. No direct OpenNebula host action is part of this procedure.
