# Ceph same-node attachment recovery

This source correction addresses a restart loop in which a replacement Pod is scheduled on the original node, its Ceph RBD disk is still attached to that VM, and the previous `VolumeAttachment` is deleting. The queue previously rejected detach because Kubernetes still desired the volume there; Kubernetes could not complete publication while that attachment object was deleting. Disabling the queue also produced misleading safe-detach history for a retained disk.

## Automatic retention contract

The controller can acknowledge logical deletion of that attachment while retaining the existing disk on the same VM. This permits external-attacher to remove its finalizer normally and Kubernetes to create a replacement attachment. The correction does not remove finalizers itself.

Automatic acknowledgement requires all of the following:

- A bound, non-deleting PV/PVC pair with matching claim UID, CSI driver and volume handle, matching RWO or RWOP access modes, and a positive PVC request no larger than the completed capacity reported by the PVC, PV and provider.
- A Ready node with the same UID and image identity recorded in persisted successful publish or stage history, without bootstrap-only provenance, one owned deleting attached VolumeAttachment, and current non-terminating consumers scheduled only on that node. RWOP additionally requires one consumer.
- Two read-only provider observations agreeing on the sole VM owner, image, disk ID, target, serial and completed capacity. The image must be persistent and stably in use; the disk must use Ceph, `CLONE=NO`, `PERSISTENT=YES` and `LN_TARGET=NONE`. Pending resize markers, unstable VM state, unknown ownership or capacity disagreement prevent acknowledgement.
- Fresh identity, demand, history and control checks after the provider observations. Active observation/recovery controls, repair state, quarantines and unreadable or malformed protection state prevent automatic retention. Explicit manual recovery retains its existing separate handler and semantics.
- The attachment reconciler must be enabled so a retained disk remains discoverable and eligible for ordinary orphan cleanup.

The automatic path performs no physical attach, detach, resize, restore or delete operation. It does not record a physical safe detach, update successful-attachment history, or clear quarantine/history. Applicable incomplete or conflicting evidence returns an actionable failure; it does not fall through to the old queue-disabled acknowledgement. Local maintenance/sticky policy, CephFS handling and explicit manual recovery are unchanged.

## Ownership and cleanup

The durable ownership source is OpenNebula's VM disk inventory, which the attachment reconciler reads independently of VolumeAttachment objects. If the old attachment disappears and no replacement publishes, the controller can restart, reconstruct ownership from that backend inventory and persisted Kubernetes state, wait the orphan grace period (120 seconds by default), and attempt ordinary detach only after current demand and protection checks allow it. For a single-writer Ceph volume whose replacement is scheduled exclusively on another node, cleanup can proceed while its Publish is queued once the original node has released the volume and the provider confirms one owner. No new in-memory hold or fabricated absence is needed. A delayed old Unpublish RPC cannot use a newly published non-deleting VolumeAttachment as authority for automatic acknowledgement or physical detach even after the consumer terminates or disappears.

Cleanup is bounded by the lifetime of the owned PV identity. The reconciler matches each inventory VM ID to one Kubernetes Node from a single VM pool observation before orphan grace starts, using exact names first and a unique case-insensitive match otherwise. It associates inventory only with this driver's PVs and VolumeAttachments, starts grace again for a different PV UID, and uses the original PV binding to identify its claim. A Released PV may be cleaned up after its claim name is reused, provided the new claim has a different UID, does not reference the original PV, and the independent release and ownership checks pass. Keep the PV and its normal deletion finalizer until physical ownership is authoritatively resolved. Ordinary CSI DeleteVolume refuses an image still owned by a VM, an offline or unreadable ownership observation, and inconsistent ownership records. It reports deletion success only after the provider establishes absence or completes ordinary deletion and observes image absence. Logical retention does not release physical ownership or authorize the PV finalizer to complete early.

The controller rechecks repair, quarantine, Pod references, node identity, volumesInUse and attachment deletion authority before each disk-detach RPC. Every single-writer Ceph background detach verifies sole ownership, even when no consumer remains. On each attempt, including retries, the ownership census precedes the final provider identity and completed-capacity checks. The final image owner list and disk target must still agree with the census. The controller then refreshes Kubernetes release and protection evidence. It rechecks persisted repair, device reports and quarantine before its final PV and claim reads to detect late protection, binding or recovery control changes. The final Kubernetes read compares the VolumeAttachment identity and deletion evidence with the earlier observation, after checking Pod release and node volumesInUse. Observed attachment changes deny the attempt. Terminating Pods still block detach. A terminal Pod without a deletion timestamp may remain after release, provided the same Ready node no longer reports the volume in use and the provider positively identifies the attachment. Background cleanup also requires a Ready node and keeps the PV UID that earned orphan grace across queue waits. Stale VolumeAttachment deletion uses UID-scoped grace, fresh PV/VA and provider observations under node-then-volume locks, and UID/resourceVersion deletion preconditions. An available historical VM pin and VA image or serial assertions must agree with the target of the absence check. Reconciliation reports a physical detach only after OpenNebula accepts the operation and readback confirms absence. An already absent disk is reported separately. Automatic local acknowledgements and physical detach verify that the provider evidence agrees with the PV backend. Unpublish verifies known historical node and VM identities before acknowledging absence.

PV and PVC UID comparisons establish the current binding. The controller rechecks persisted protection, the binding and recovery controls then refreshes Pod demand and the original Node’s UID, Ready condition, and deletion state. It confirms that the deleting VolumeAttachment still has the same UID and authority before acknowledging AUTO retention. An earlier static rebind to a different PV or PVC name is allowed when the current binding and VA claim assertions agree and the provider image, device and original node match the history pins. Historical claim names describe the prior binding.

The provider and Kubernetes observations are bounded checks. Controller locks serialize this controller's operations. External Kubernetes and OpenNebula actors can still change attachment state after the last read. Same-node physical retention does not prove an old process stopped, inspect guest mounts, revoke writer access or fence database writers. Database/operator fencing and the ordinary publish/node-stage identity checks remain necessary. Pending or terminal Pod phase alone is not proof that a disk is unmounted.

## Publish after Node replacement

A Publish to a replacement Kubernetes Node with the same name is a separate
Ceph operation. On OpenNebula 7.4.0, the controller may record one pending
attach attempt in the existing volume history after verifying the binding,
demand, prior release, and settled provider absence. The attach template
carries an opaque `CSI_ATTACH_ATTEMPT` value and keeps the existing device
serial. A restarted controller can complete Publish when the sole selected
disk carries that exact value and the original admission still matches.
An unresolved attempt remains held across timeouts and restarts. Elapsed time
and an idle snapshot alone cannot prove that a previously admitted request
will never reach the provider. The attempt supplies no authority for Stage,
retention, physical detach, or DeleteVolume.

## Validation and rollout

Public CSI regressions cover both queue modes, positive retained ownership, history preservation, unknown and conflicting evidence, late identity/control changes, cancellation, explicit manual behavior, delayed old RPCs, and restart/orphan cleanup without another Unpublish request. Provider protocol tests exercise actual XML-RPC parsing and assert zero mutations for positive and denied observations.

These local tests are separate from publication and hplzdev runtime acceptance. Validate a new immutable candidate on disposable hplzdev Ceph volumes, including checksum reads during deletion/republication, ordinary cleanup and preservation of existing resources. Do not reuse earlier RC3 receipts as proof for the new candidate. This correction does not qualify OpenNebula 7.4.0 persistent SYSTEM-target LVM expansion or waive the existing production rollout/observation gates. No direct OpenNebula host action is part of this procedure.
