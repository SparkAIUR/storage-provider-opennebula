package driver

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type publishMoveDetach struct {
	VAName, Marker, Grant string
	VAUID                 types.UID
}

func publishMoveGrantSnapshot(binding *VolumeRuntimeContext) string {
	raw, present := binding.PVAnnotations[annotationAllowCrossNodeUntil]
	return publishMoveDigest([]any{present, raw})
}

func (s *ControllerServer) checkPublishMoveScope(ctx context.Context, volume string, move *pendingPublishMove, history VolumeHistoryRecord) (publishPVAdmission, error) {
	deny := attachmentEvidenceUnavailable("pending Publish move scope is unverified", nil)
	runtime := s.driver.kubeRuntime
	if runtime == nil || !runtime.enabled || history.PendingReplacementAttach != nil || !move.matchesHistory(history) ||
		(s.driver.maintenanceMode != nil && s.driver.maintenanceMode.Active()) {
		return publishPVAdmission{}, deny
	}
	binding, err := runtime.ResolveVolumeRuntimeContext(ctx, volume)
	if err != nil || binding == nil || move.Binding == "" || move.Binding != publishMoveBinding(binding, s.driver.name, volume) ||
		!move.ownsMarker(binding.PVAnnotations[annotationPublishMoveReceipt]) {
		return publishPVAdmission{}, deny
	}
	if backend, err := binding.verifiedAttachmentBackend(); err != nil || backend != "local" {
		return publishPVAdmission{}, deny
	}
	control := resolvedRecoveryControlState(volume, binding, VolumeRecoveryControlState{})
	if control.Invalid || control.Active() {
		return publishPVAdmission{}, deny
	}
	for _, target := range []opennebula.HostArtifactAttachmentTarget{
		{NodeName: move.Source.Name, VMID: move.Source.VM, ImageID: move.Image, DiskID: -1},
		{NodeName: move.Destination.Name, VMID: move.Destination.VM, ImageID: move.Image, DiskID: move.Slot},
	} {
		if err := s.attachmentProtectionChecks(ctx, volume, binding, target); err != nil {
			return publishPVAdmission{}, err
		}
	}
	return s.publishPVImageCheck(ctx, volume, move.Image, binding, history)
}

func (s *ControllerServer) verifyPublishMoveRelease(ctx context.Context, volume string, image int, expected publishMoveNode) error {
	deny := attachmentEvidenceUnavailable("pending Publish move VM release is unverified", nil)
	node, err := s.publishMoveNode(ctx, expected.Name)
	if err != nil || node != expected {
		return deny
	}
	inspector, ok := s.volumeProvider.(opennebula.ReplacementAttachInspector)
	if !ok {
		return deny
	}
	name, err := inspector.InspectReplacementRelease(ctx, image, node.VM, "")
	if err != nil || name == "" {
		return deny
	}
	if _, err := s.volumeProvider.GetVolumeInNode(opennebula.WithExpectedImageID(ctx, image), image, node.VM); !errors.Is(err, opennebula.ErrAttachmentNotFound) {
		return deny
	}
	observed, err := s.publishMoveNode(ctx, expected.Name)
	if err != nil || observed != expected {
		return deny
	}
	return ctx.Err()
}

func (s *ControllerServer) publishMoveDeletingVA(ctx context.Context, volume string, move *pendingPublishMove, binding *VolumeRuntimeContext, expected *publishMoveDetach) (*publishMoveDetach, error) {
	deny := attachmentEvidenceUnavailable("pending Publish destination VolumeAttachment is unverified", nil)
	name := csiVolumeAttachmentName(volume, s.driver.name, move.Destination.Name)
	if expected != nil && (expected.VAName != name || expected.VAUID == "") {
		return nil, deny
	}
	attachments, err := s.detachVolumeAttachments(ctx, binding.PVName, "")
	if err != nil {
		return nil, err
	}
	client := s.driver.kubeRuntime.client.StorageV1().VolumeAttachments()
	va, err := client.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && len(attachments) == 0 && expected != nil {
		return nil, nil
	}
	observed, found := attachments[name]
	if err != nil || !found || len(attachments) != 1 || va.UID == "" || va.UID != observed.uid ||
		va.DeletionTimestamp.IsZero() || !matchesVolumeAttachment(binding.PVName, va) ||
		va.Spec.Attacher != s.driver.name || va.Spec.NodeName != move.Destination.Name ||
		!slices.Contains(va.Finalizers, "external-attacher/"+strings.ReplaceAll(s.driver.name, "/", "-")) ||
		validateAttachmentImageAssertions(va.Status.AttachmentMetadata, move.Image) != nil {
		return nil, deny
	}
	if target, present := va.Status.AttachmentMetadata["volumeName"]; present && target != move.Target {
		return nil, deny
	}
	proof := publishMoveDetach{VAName: name, VAUID: va.UID, Marker: binding.PVAnnotations[annotationPublishMoveReceipt], Grant: publishMoveGrantSnapshot(binding)}
	if expected != nil && *expected != proof {
		return nil, deny
	}
	return &proof, ctx.Err()
}

func (s *ControllerServer) preparePublishMoveDetach(ctx context.Context, volume, node string, identity physicalDetachIdentity) (*pendingPublishMove, *publishMoveDetach, error) {
	deny := attachmentEvidenceUnavailable("pending Publish destination detach is unverified", nil)
	move := identity.publishMove
	if move == nil {
		return nil, nil, nil
	}
	if move.Detach != nil || move.Destination.Name != node || move.Destination.VM != identity.vmID ||
		move.Destination.UID != string(identity.nodeUID) || move.Image != identity.imageID ||
		move.Slot != identity.diskID || identity.backend != "local" {
		return nil, nil, deny
	}
	history, err := s.readAttachmentHistory(ctx, volume)
	if err != nil || !samePendingPublishMove(history.PendingPublishMove, move) {
		return nil, nil, deny
	}
	admission, err := s.checkPublishMoveScope(ctx, volume, move, history)
	if err != nil {
		return nil, nil, err
	}
	if err := s.verifyPublishMoveRelease(ctx, volume, move.Image, move.Source); err != nil {
		return nil, nil, err
	}
	destination, err := s.publishMoveNode(ctx, node)
	if err != nil || destination != move.Destination {
		return nil, nil, deny
	}
	disks, ok := s.volumeProvider.(opennebula.VolumeAttachmentInspector)
	if !ok {
		return nil, nil, deny
	}
	metadata, err := disks.InspectVolumeAttachment(opennebula.WithExpectedImageID(ctx, move.Image), volume, node)
	if err != nil || metadata == nil || metadata.ImageID != move.Image || metadata.ImageName != volume ||
		metadata.RequestedNodeID != move.Destination.VM || !metadata.AttachedToRequestedNode || metadata.ImageRunningVMs != 1 ||
		len(metadata.ImageVMIDs) != 1 || metadata.ImageVMIDs[0] != move.Destination.VM ||
		len(metadata.DiskRecords) != 1 || metadata.VerifyOwnership() != nil {
		return nil, nil, deny
	}
	disk := metadata.DiskRecords[0]
	if disk.NodeID != move.Destination.VM || disk.DiskID != move.Slot || disk.Target != move.Target ||
		!opennebula.DeviceSerialMatchesImage(disk.Serial, move.Image) || disk.ReadOnly || disk.ReplacementMarked {
		return nil, nil, deny
	}
	if err := s.checkPhysicalDetachAuthority(ctx, volume, node, &identity, detachDispatch); err != nil {
		return nil, nil, err
	}
	proof, err := s.publishMoveDeletingVA(ctx, volume, move, admission.binding, nil)
	if err != nil {
		return nil, nil, err
	}
	if proof.VAUID != identity.attachments[proof.VAName].uid {
		return nil, nil, deny
	}
	if err := admission.check(ctx, admission.binding); err != nil {
		return nil, nil, err
	}
	return move, proof, nil
}

func (s *ControllerServer) recordPublishMoveDetach(ctx context.Context, volume string, move *pendingPublishMove, proof *publishMoveDetach) error {
	deny := attachmentEvidenceUnavailable("pending Publish detach acceptance could not be persisted", nil)
	_, err := s.driver.volumeHistory.updateHistory(ctx, volume, true, func(history *VolumeHistoryRecord) error {
		if !samePendingPublishMove(history.PendingPublishMove, move) {
			return deny
		}
		admission, err := s.checkPublishMoveScope(ctx, volume, move, *history)
		if err != nil {
			return err
		}
		va, err := s.publishMoveDeletingVA(ctx, volume, move, admission.binding, proof)
		if err != nil || va == nil {
			return deny
		}
		if err := admission.check(ctx, admission.binding); err != nil {
			return err
		}
		updated := *move
		updated.Detach = proof
		history.PendingPublishMove = &updated
		return nil
	})
	if err != nil {
		return deny
	}
	return nil
}

func (s *ControllerServer) publishMoveWaitingNode(ctx context.Context, volume string, move *pendingPublishMove, successor detachAttachmentAuthority) (*corev1.Node, int, error) {
	deny := attachmentEvidenceUnavailable("Publish successor Node is unverified", nil)
	node, err := s.driver.kubeRuntime.client.CoreV1().Nodes().Get(ctx, successor.node, metav1.GetOptions{})
	ready, _ := nodeReadyConditionStatus(node)
	if err != nil || node == nil || node.Name != successor.node || node.UID == "" || node.DeletionTimestamp != nil ||
		ready != corev1.ConditionTrue || node.CreationTimestamp.IsZero() || !successor.created.After(node.CreationTimestamp.Time) ||
		successor.created.After(time.Now()) || nodeReportsVolumeInUse(node, s.driver.name, volume) {
		return nil, 0, deny
	}
	switch node.Name {
	case move.Source.Name:
		if string(node.UID) == move.Source.UID {
			return node, move.Source.VM, nil
		}
	case move.Destination.Name:
		if string(node.UID) == move.Destination.UID {
			return node, move.Destination.VM, nil
		}
	default:
		target, err := s.publishMoveNode(ctx, node.Name)
		if err == nil && target.UID == string(node.UID) && target.UID != move.Source.UID && target.UID != move.Destination.UID &&
			target.VM != move.Source.VM && target.VM != move.Destination.VM {
			if err := s.verifyPublishMoveRelease(ctx, volume, move.Image, target); err != nil {
				return nil, 0, err
			}
			return node, target.VM, nil
		}
	}
	return nil, 0, deny
}

func (s *ControllerServer) recheckPublishMoveSuccessor(ctx context.Context, pvName, oldName, name string, successor detachAttachmentAuthority) error {
	vas := s.driver.kubeRuntime.client.StorageV1().VolumeAttachments()
	if name != oldName {
		if _, err := vas.Get(ctx, oldName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return attachmentEvidenceUnavailable("old Publish VolumeAttachment has not retired", err)
		}
	}
	va, err := vas.Get(ctx, name, metav1.GetOptions{})
	if err != nil || va == nil || va.Name != name || va.Spec.Source.PersistentVolumeName == nil ||
		*va.Spec.Source.PersistentVolumeName != pvName || va.Spec.Source.InlineVolumeSpec != nil ||
		!reflect.DeepEqual(successor, detachAttachmentSnapshot(va)) {
		return attachmentEvidenceUnavailable("Publish successor VolumeAttachment is unverified", err)
	}
	return ctx.Err()
}

func (s *ControllerServer) settlePublishMoveDetach(ctx context.Context, volume string, move *pendingPublishMove, requireRetired bool) error {
	deny := attachmentEvidenceUnavailable("pending Publish destination release is unverified", nil)
	if move.Detach == nil {
		return deny
	}
	verify := func(history VolumeHistoryRecord, retiring bool) (bool, error) {
		if !samePendingPublishMove(history.PendingPublishMove, move) {
			return false, deny
		}
		admission, err := s.checkPublishMoveScope(ctx, volume, move, history)
		if err != nil {
			return false, err
		}
		binding := admission.binding
		if binding.PVAnnotations[annotationPublishMoveReceipt] != move.Detach.Marker ||
			publishMoveGrantSnapshot(binding) != move.Detach.Grant {
			return false, deny
		}
		for _, node := range []publishMoveNode{move.Source, move.Destination} {
			if err := s.verifyPublishMoveRelease(ctx, volume, move.Image, node); err != nil {
				return false, err
			}
		}
		inspector, ok := s.volumeProvider.(opennebula.VolumeAttachmentInspector)
		if !ok {
			return false, deny
		}
		metadata, err := inspector.InspectVolumeAttachment(opennebula.WithExpectedImageID(ctx, move.Image), volume, move.Destination.Name)
		if err != nil || metadata == nil || metadata.ImageID != move.Image || metadata.ImageName != volume ||
			metadata.RequestedNodeID != move.Destination.VM || metadata.ImageRunningVMs != 0 ||
			len(metadata.ImageVMIDs) != 0 || len(metadata.DiskRecords) != 0 || metadata.VerifyOwnership() != nil {
			return false, deny
		}
		identity := physicalDetachIdentity{pvUID: binding.pvUID, nodeUID: types.UID(move.Destination.UID),
			imageID: move.Image, vmID: move.Destination.VM, diskID: move.Slot, backend: "local", publishMove: move}
		check := detachReadback
		if requireRetired {
			check = detachPublishMoveRetirement
		}
		if err := s.checkPhysicalDetachAuthority(ctx, volume, move.Destination.Name, &identity, check); err != nil {
			return false, err
		}
		finish := func(gone bool) (bool, error) {
			if gone && retiring {
				if err := s.driver.stickyAttachments.clearReleasedPublishMoveGrace(ctx, volume, move.Destination.Name, binding); err != nil {
					return false, err
				}
			}
			return gone, admission.check(ctx, binding)
		}
		if requireRetired {
			for _, va := range identity.attachments {
				if !va.deleting {
					return finish(true)
				}
			}
		}
		va, err := s.publishMoveDeletingVA(ctx, volume, move, binding, move.Detach)
		if err != nil {
			return false, err
		}
		return finish(va == nil)
	}
	history, err := s.readAttachmentHistory(ctx, volume)
	if err != nil {
		return deny
	}
	gone, err := verify(history, false)
	if err != nil {
		return err
	}
	if !gone {
		if requireRetired {
			return attachmentEvidenceUnavailable("waiting for the deleting Publish VolumeAttachment to retire", nil)
		}
		return nil
	}
	_, err = s.driver.volumeHistory.updateHistory(ctx, volume, true, func(history *VolumeHistoryRecord) error {
		gone, err := verify(*history, true)
		if err != nil || !gone {
			return deny
		}
		history.PendingPublishMove = nil
		history.LastPublishMoveReceipt = move.Detach.Marker
		if now := time.Now().UTC(); now.After(history.LastSafeDetachTime) {
			history.LastSafeDetachNodeName, history.LastSafeDetachTime = move.Destination.Name, now
		}
		return nil
	})
	if err != nil {
		return deny
	}
	return nil
}

func (s *ControllerServer) retryPublishMoveDetach(ctx context.Context, volume string, move *pendingPublishMove, requireRetired bool) error {
	releaseNode := s.driver.operationLocks.Acquire(controllerNodeLockKey(move.Destination.Name))
	defer releaseNode()
	releaseVolume := s.driver.operationLocks.Acquire(controllerVolumeLockKey(volume))
	defer releaseVolume()
	history, err := s.readAttachmentHistory(ctx, volume)
	if err != nil {
		return attachmentEvidenceUnavailable("pending Publish destination history is unverified", err)
	}
	if history.PendingPublishMove == nil && history.LastPublishMoveReceipt == move.Detach.Marker {
		return attachmentEvidenceUnavailable("Publish move already retired; retry the request", nil)
	}
	return s.settlePublishMoveDetach(ctx, volume, move, requireRetired)
}
