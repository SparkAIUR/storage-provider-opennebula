package driver

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func nodeReportsVolumeInUse(node *corev1.Node, driver, volume string) bool {
	name := corev1.UniqueVolumeName(fmt.Sprintf("kubernetes.io/csi/%s^%s", driver, volume))
	return slices.Contains(node.Status.VolumesInUse, name)
}

func (s *ControllerServer) checkReplacedNodeStaleAttachment(ctx context.Context, volume, nodeName string, imageID, vmID int, history VolumeHistoryRecord, expected *storagev1.VolumeAttachment) (bool, error) {
	if history.LastSuccessfulNodeName != nodeName || history.LastSuccessfulNodeUID == "" || !cephHistoryHasObservedAuthority(history) {
		return false, nil
	}
	runtime := s.driver.kubeRuntime
	core := runtime.client.CoreV1()
	node, err := core.Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return true, attachmentEvidenceUnavailable("replacement node identity is unreadable", err)
	}
	if string(node.UID) == history.LastSuccessfulNodeUID {
		return false, nil
	}
	deny := func(err error) (bool, error) {
		return true, attachmentEvidenceUnavailable("stale replacement attachment release is unverified", err)
	}
	if history.PendingReplacementAttach != nil || vmID <= 0 || history.LastSuccessfulOpenNebulaVMID <= 0 ||
		imageID <= 0 || history.LastSuccessfulImageID != imageID || history.LastSuccessfulDiskID == nil ||
		history.LastSuccessfulTarget == "" || history.LastSuccessfulDeviceSerial != fmt.Sprintf("onecsi-%d", imageID) ||
		node.UID == "" || node.DeletionTimestamp != nil || node.CreationTimestamp.IsZero() ||
		!slices.ContainsFunc(node.Status.Conditions, func(c corev1.NodeCondition) bool {
			return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
		}) {
		return deny(nil)
	}
	if observed := history.LastSuccessfulPublishTime; !observed.IsZero() && !observed.Before(node.CreationTimestamp.Time) {
		return deny(nil)
	}
	if observed := history.LastSuccessfulStageTime; !observed.IsZero() && !observed.Before(node.CreationTimestamp.Time) {
		return deny(nil)
	}
	pv, err := s.ownedDeletionPV(ctx, volume)
	if err != nil || pv == nil || pv.Name != history.PVName || pv.DeletionTimestamp != nil ||
		pv.Status.Phase != corev1.VolumeBound || pv.CreationTimestamp.IsZero() || !cephRetentionAccessMode(pv.Spec.AccessModes) {
		return deny(err)
	}
	backend, err := persistentVolumeBackend(pv)
	if err != nil || !isCephBackend(backend) {
		return deny(err)
	}
	pvc, replaced, err := runtime.attachmentClaim(ctx, pv)
	if err != nil || replaced || pvc == nil || pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Namespace != history.PVCNamespace || pvc.Name != history.PVCName || pvc.CreationTimestamp.IsZero() ||
		!slices.Equal(pvc.Spec.AccessModes, pv.Spec.AccessModes) {
		return deny(err)
	}

	vas := runtime.client.StorageV1().VolumeAttachments()
	attachments, err := s.detachVolumeAttachments(ctx, pv.Name, "")
	if err != nil || len(attachments) != 1 {
		return deny(err)
	}
	var va *storagev1.VolumeAttachment
	for name := range attachments {
		va, err = vas.Get(ctx, name, metav1.GetOptions{})
	}
	if err != nil || va == nil || !matchesVolumeAttachment(pv.Name, va) || va.Spec.Attacher != s.driver.name ||
		va.Spec.NodeName != nodeName || va.UID == "" || va.ResourceVersion == "" || !va.Status.Attached ||
		va.CreationTimestamp.IsZero() || !va.CreationTimestamp.Before(&node.CreationTimestamp) ||
		va.CreationTimestamp.Before(&pvc.CreationTimestamp) || va.CreationTimestamp.Before(&pv.CreationTimestamp) ||
		validateAttachmentImageAssertions(va.Status.AttachmentMetadata, imageID) != nil {
		return deny(err)
	}
	if expected == nil {
		if va.DeletionTimestamp == nil {
			return deny(nil)
		}
	} else if va.Name != expected.Name || va.UID != expected.UID || va.ResourceVersion != expected.ResourceVersion || va.DeletionTimestamp != nil {
		return deny(nil)
	}
	if target, ok := va.Status.AttachmentMetadata["volumeName"]; ok &&
		strings.TrimPrefix(strings.TrimSpace(target), "/dev/") != history.LastSuccessfulTarget {
		return deny(nil)
	}
	runtimeCtx := &VolumeRuntimeContext{PVName: pv.Name, PVCNamespace: pvc.Namespace, PVCName: pvc.Name,
		PVCUID: pvc.UID, verifiedClaimBinding: true, PVAnnotations: pv.Annotations, PVCAnnotations: pvc.Annotations,
		Backend: backend, AccessModes: pv.Spec.AccessModes}
	if control := resolvedRecoveryControlState(volume, runtimeCtx, VolumeRecoveryControlState{}); control.Invalid || control.Active() {
		return deny(nil)
	}
	inspector, ok := s.volumeProvider.(opennebula.ReplacementAttachInspector)
	if !ok {
		return deny(nil)
	}
	if disk, err := inspector.InspectReplacementAttach(ctx, volume, nodeName, imageID, vmID, ""); err != nil || disk != nil {
		return deny(err)
	}
	target := opennebula.HostArtifactAttachmentTarget{NodeName: nodeName, ImageID: imageID, VMID: vmID, DiskID: -1}
	if originalVM := history.LastSuccessfulOpenNebulaVMID; originalVM != vmID {
		if _, err := inspector.InspectReplacementRelease(ctx, imageID, originalVM, ""); err != nil {
			return deny(err)
		}
		original := target
		original.VMID, original.DiskID = originalVM, *history.LastSuccessfulDiskID
		if err := s.attachmentProtectionChecks(ctx, volume, runtimeCtx, original); err != nil {
			return true, err
		}
	} else {
		target.DiskID = *history.LastSuccessfulDiskID
	}
	protection, err := s.localRWOProtectionProjection(ctx, volume, nodeName, runtimeCtx, false, true)
	if err != nil || protection.Invalid || protection.Protected {
		return deny(err)
	}
	if err := s.recheckAttachmentAuthority(ctx, volume, runtimeCtx, target, pv, pvc, false); err != nil {
		return true, err
	}
	currentNode, err := core.Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil || currentNode.UID != node.UID || currentNode.DeletionTimestamp != nil ||
		!currentNode.CreationTimestamp.Equal(&node.CreationTimestamp) ||
		!slices.ContainsFunc(currentNode.Status.Conditions, func(c corev1.NodeCondition) bool {
			return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
		}) {
		return deny(err)
	}
	waiting, err := s.replacementWaitingConsumers(ctx, pv, pvc, currentNode, false)
	if err != nil || (!waiting && nodeReportsVolumeInUse(currentNode, s.driver.name, volume)) {
		return deny(err)
	}
	currentHistory, err := s.readAttachmentHistory(ctx, volume)
	if err != nil || currentHistory.PendingReplacementAttach != nil ||
		replacementHistoryGeneration(currentHistory) != replacementHistoryGeneration(history) {
		return deny(err)
	}
	if err := s.recheckDetachVolumeAttachments(ctx, pv.Name, "", attachments); err != nil {
		return true, err
	}
	currentVA, err := vas.Get(ctx, va.Name, metav1.GetOptions{})
	if err != nil || currentVA.UID != va.UID || currentVA.ResourceVersion != va.ResourceVersion ||
		!reflect.DeepEqual(currentVA.Spec, va.Spec) || !reflect.DeepEqual(currentVA.Status, va.Status) ||
		!reflect.DeepEqual(currentVA.DeletionTimestamp, va.DeletionTimestamp) {
		return deny(err)
	}
	if originalVM := history.LastSuccessfulOpenNebulaVMID; originalVM != vmID {
		original := target
		original.VMID, original.DiskID = originalVM, *history.LastSuccessfulDiskID
		if err := s.attachmentProtectionChecks(ctx, volume, runtimeCtx, original); err != nil {
			return true, err
		}
	}
	if err := s.recheckAttachmentAuthority(ctx, volume, runtimeCtx, target, pv, pvc, false); err != nil {
		return true, err
	}
	return true, nil
}
