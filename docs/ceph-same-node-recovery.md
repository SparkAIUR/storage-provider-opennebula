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

AUTO requires a positive original provider VM ID in observed history,
including records with only a successful Stage timestamp. A legacy row without
that pin cannot establish same-VM continuity.

## Publish after Node replacement

A Publish to a replacement Kubernetes Node with the same name is a separate
Ceph operation. On OpenNebula 7.4.0, the controller may record one pending
attach attempt in the existing volume history after verifying the binding,
demand, prior release, and settled provider absence. The replacement Node
may use the original VM or a different verified VM. For a different VM,
the controller independently verifies the original VM identity and release
before dispatch and again before completing Publish. The attach template
carries an opaque `CSI_ATTACH_ATTEMPT` value and keeps the existing device
serial. A restarted controller can complete Publish when the sole selected
disk carries that exact value and the original admission still matches.
A prepared attempt can resume after a restart. Each caller proposes a new
attempt ID in the conditional update that advances it to dispatch-possible.
Only the acknowledged winner can send the attach, after reading back the exact
record and rechecking current authority. Replacement dispatch requires the
same owned, unattached VolumeAttachment throughout. If the authorization
update's reply is lost, the same caller can retire its exact ID after independent readback because it knows
it has sent no attach. This retirement can finish after the request is canceled
or demand ends. A prepared attempt can also retire after verified
abandonment and a conditional update that rules out concurrent arming.

If the marked attach takes effect and its reply is lost, a new waiting Pod
on the admitted replacement Node can coexist with logical deletion of the
exact pinned VolumeAttachment. After that attachment UID is gone, the same
recovery can acknowledge the sole verified successor under the same name if
it enters deletion.
The controller rechecks the binding, demand
and protections and observes the sole marked disk twice, including its serial,
slot and completed capacity. This acknowledgement leaves the disk, pending
attempt and successful history intact. Once the pinned attachment UID is gone,
Publish can complete against an eligible owned successor on that Node after
rechecking the same disk and current authority. If controller expansion has
finished and the device matches the PV capacity, marked retention and successor
Publish can proceed while kubelet is still waiting to resize the filesystem.
Ordinary same-node retention still requires completed claim and device capacity.
Only verified Publish advances the successful attachment history. These recovery steps
leave provider state unchanged.
[Kubernetes filesystem expansion](https://v1-35.docs.kubernetes.io/docs/concepts/storage/persistent-volumes/#resizing-a-volume-containing-a-file-system)

If demand disappears after dispatch, Unpublish or the reconciler can detach
only the exact marked disk after verifying current release. For a single
verified remote destination, the reconciler can release the disk when the source
attachment has retired, the exact pinned source attachment is deleting, or a
verified same-name successor is deleting after retirement of the pinned UID.
For a deleting attachment, Unpublish verifies its detach receipt so the
external-attacher can remove its finalizer. If the destination Pod is canceled
first, Unpublish can acknowledge the exact deleting, unattached destination
after independently verifying release on its live VM. A renewed consumer must
still be waiting for its mount. For a terminating or Released claim the driver
uses the pinned original binding and checks that no consumer may still use the
destination. With a prepared attempt, the provider must show zero owners.
Destination acknowledgement leaves the source attempt and any receipt in place.
An owned Released
PV can use its original claim reference after the claim is gone, including when
a different claim UID has reused the name and is verifiably bound to another PV.
Idle snapshots alone do not prove that a possibly sent attach cannot still take effect.
Unpublish and DeleteVolume stay blocked while that attempt is unresolved.

After a verified physical detach for a deleting VolumeAttachment, the controller
keeps completion in the same record, scoped to the actual deleting
VolumeAttachment UID. This can be the verified successor after the original
attachment has retired. A retry for the exact still-deleting VolumeAttachment rechecks identity, release, protection and provider absence
before acknowledging it. A positively identified, unstarted Pod on the replacement
Node can wait during this acknowledgement. Kubelet can set Pod `StartTime`
and default container waiting states before it waits for volumes to attach and
mount. Those fields can coexist with `volumesInUse` demand. Physical detach
still requires release of Pod demand and mount use.
[Kubernetes 1.35 kubelet sync order](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/kubelet/kubelet.go#L1871-L1962)
[Kubelet status timestamps](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/kubelet/status/status_manager.go#L761-L768)
[Default waiting states](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/kubelet/kubelet_pods.go#L2253-L2266)

Publish, DeleteVolume and the reconciler can retire the
completion after verifying that the original VolumeAttachment UID is gone,
even when its PV has disappeared or been replaced. Cleanup without a
VolumeAttachment retires the record after verified detach. Retirement preserves
the original Publish and Stage history.

The caller that verified the physical detach can retry a conflicting history
write after rechecking the original attempt and provider absence. The receipt
keeps the identities admitted for the detach. Before acknowledging Unpublish,
the controller checks the current binding, attachment, release and protection.
A change can block acknowledgement even after the history write commits.
Independent readback can confirm a committed receipt whose write reply was lost.
For a replacement detach, the driver records the provider's successful reply
against the exact attempt, source attachment and disk identity before polling for
release. This record allows a later controller request or reconciliation to check
settlement without sending another detach. It does not authorize Unpublish
success or publication on its own. The existing bounded poll covers stale image
ownership and original VM settlement. Completion requires fresh provider absence
and verified original VM release, along with the current Kubernetes scope and
per-volume controls. The acceptance write preserves the exact reply across
control changes and verified claim or attachment turnover while keeping the
original detach scope. Settlement refreshes controls, then confirms the Pod,
Node and attachment evidence again. These bounded observations do not fence
provider or Kubernetes changes.

When a detach was accepted without a source VolumeAttachment, reconciliation
or a fresh Publish can retire that record after settlement for a successor with
the same canonical attachment name and a new UID whose consumer is still
waiting to mount. If that successor is deleting and unattached, Unpublish can
acknowledge it after verifying release and settled absence. The accepted record
remains until its normal retirement checks pass. Each new publication requires
its own attach authorization. An uncertain reply or a failed acceptance write
leaves the attempt unresolved. Global local-disk maintenance does not govern
these verified Ceph paths.

An attached VolumeAttachment that predates a verified same-name Node
replacement can converge after the controller proves release on both the pinned
original VM and the current VM. Unpublish can acknowledge the exact deleting
attachment. The reconciler can request deletion of the exact stale attachment
after its grace period. Neither path changes provider state.

## Validation and rollout

Public CSI regressions cover both queue modes, positive retained ownership, history preservation, unknown and conflicting evidence, late identity/control changes, cancellation, explicit manual behavior, delayed old RPCs, and restart/orphan cleanup without another Unpublish request. Provider protocol tests exercise actual XML-RPC parsing and assert zero mutations for positive and denied observations.

These local tests are separate from publication and hplzdev runtime acceptance. Validate a new immutable candidate on disposable hplzdev Ceph volumes, including checksum reads during deletion/republication, ordinary cleanup and preservation of existing resources. Do not reuse earlier RC3 receipts as proof for the new candidate. This correction does not qualify OpenNebula 7.4.0 persistent SYSTEM-target LVM expansion or waive the existing production rollout/observation gates. No direct OpenNebula host action is part of this procedure.
